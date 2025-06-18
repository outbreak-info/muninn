import csv
import time
from typing import Set, Any, Dict

import polars as pl

from DB.inserts.file_parsers.file_parser import FileParser
from DB.inserts.geo_locations import find_or_insert_geo_location
from DB.inserts.samples import copy_insert_samples
from DB.models import GeoLocation
from DB.queries.samples import get_samples_accession_and_id_as_pl_df
from utils.constants import StandardColumnNames, COLLECTION_DATE, GEO_LOCATION
from utils.dates_and_times import parse_collection_start_and_end


class SC2SamplesParser(FileParser):
    def __init__(self, filename: str):
        self.filename = filename
        self.delimiter = '\t'
        self._verify_header()

    async def parse_and_insert(self):
        # Scan file, rename columns, drop unused cols, drop rows with null collection date
        samples_input = (
            pl.scan_csv(self.filename, separator=self.delimiter)
            .rename({old: new for new, old in column_name_map.items()})
            .select(set(column_name_map.keys()))
            .drop_nulls([pl.col(COLLECTION_DATE)])
            .cast(
                {
                    StandardColumnNames.release_date: pl.Datetime,
                    StandardColumnNames.creation_date: pl.Datetime
                }
            )
            .with_columns(
                pl.col(StandardColumnNames.bio_project).fill_null('NA')
            )
        )
        # todo: unique? No, leave it out for now to force errors on conflict.

        # Add in not-null columns with placeholder values
        samples_padded = (
            samples_input
            .join(SC2SamplesParser._get_placeholder_value_map(), how='cross')
        )

        geo_locations = await SC2SamplesParser._insert_geo_locations(samples_input)
        existing_samples = await get_samples_accession_and_id_as_pl_df()

        samples_final = (
            samples_padded
            .join(geo_locations.lazy(), on=pl.col(GEO_LOCATION), how='left')
            .drop(pl.col(GEO_LOCATION))
            .with_columns(
                pl.col(COLLECTION_DATE).map_elements(
                    parse_collection_start_and_end,
                    return_dtype=pl.List(pl.Date)
                )
            )
            .with_columns(
                pl.col(COLLECTION_DATE).list.to_struct(
                    fields=[StandardColumnNames.collection_start_date, StandardColumnNames.collection_end_date]
                )
            )
            .unnest(COLLECTION_DATE)
        )

        new_samples = samples_final.join(
            existing_samples.lazy(),
            on=pl.col(StandardColumnNames.accession),
            how='anti'
        )
        copy_status = await copy_insert_samples(new_samples.collect())
        print(f'new samples: {copy_status}')


        updated_samples = samples_final.join(
            existing_samples.lazy(),
            on=pl.col(StandardColumnNames.accession),
             how='inner'
        )






    @staticmethod
    async def _insert_geo_locations(samples_input: pl.LazyFrame) -> pl.DataFrame:
        """
        Insert geo_locations from samples, return original geo_location strings and db ids
        :param samples_input:
        :return: geo_location <str>, id <int> to be joined with samples
        """
        start = time.perf_counter()
        geo_locations = (
            samples_input
            .select(pl.col(GEO_LOCATION))
            .unique()
            .drop_nulls()
            .with_columns(
                pl.col(GEO_LOCATION).str.split('/').list.to_struct(
                    n_field_strategy="max_width",
                    fields=[
                        StandardColumnNames.country_name,
                        StandardColumnNames.admin1_name,
                        StandardColumnNames.admin2_name,
                        StandardColumnNames.admin3_name
                    ]
                )
                .alias('tmp_geo_struct')
            )
            .unnest('tmp_geo_struct')
            .collect()
        )

        ids = []
        for row in geo_locations.iter_rows(named=True):
            ids.append(
                await find_or_insert_geo_location(
                    GeoLocation(
                        country_name=row[StandardColumnNames.country_name],
                        admin1_name=row[StandardColumnNames.admin1_name],
                        admin2_name=row[StandardColumnNames.admin2_name],
                        admin3_name=row[StandardColumnNames.admin3_name]
                    )
                )
            )

        geo_locations = (
            geo_locations
            .select(pl.col(GEO_LOCATION))
            .with_columns(pl.Series(ids).alias(StandardColumnNames.geo_location_id))
        )
        print(f'geo locations took {round(time.perf_counter() - start, 2)}s')
        return geo_locations

    @staticmethod
    def _get_placeholder_value_map() -> pl.LazyFrame:
        placeholders: Dict[str, Any] = {cn: 'NA' for cn in fields_not_present_not_null}
        placeholders[StandardColumnNames.bytes] = -1
        placeholders[StandardColumnNames.is_retracted] = False
        return pl.LazyFrame(placeholders)

    def _verify_header(self):
        with open(self.filename, 'r') as f:
            reader = csv.DictReader(f, delimiter=self.delimiter)
            required_columns = set(column_name_map.values())
            if not set(reader.fieldnames) >= required_columns:
                raise ValueError(f'Missing required fields: {required_columns - set(reader.fieldnames)}')

    @classmethod
    def get_required_column_set(cls) -> Set[str]:
        pass


"""
metadata
    fields used
        Accession
        Bioprojects
        Biosample
        Collection_Date
        Geographic_Location
        Host_OrganismName
        Isolate_Name
        Isolate_Source
        Length
        ReleaseDate
        UpdateDate
        Virus_OrganismName
    fields unused
        Geographic_Region
        Host_TaxID
        Nucleotide_SequenceHash
        PurposeOfSampling
        SraAccessions
        Submitter_Country
        USA_State
        Virus_PangolinClassification
        Virus_TaxId
"""

column_name_map = {
    StandardColumnNames.accession: 'Accession',
    StandardColumnNames.bio_project: 'Bioprojects',
    StandardColumnNames.bio_sample: 'Biosample',
    StandardColumnNames.host: 'Host_OrganismName',
    StandardColumnNames.isolate: 'Isolate_Name',
    StandardColumnNames.organism: 'Virus_OrganismName',
    StandardColumnNames.isolation_source: 'Isolate_Source',
    COLLECTION_DATE: 'Collection_Date',
    StandardColumnNames.release_date: 'ReleaseDate',
    StandardColumnNames.creation_date: 'UpdateDate',  # todo
    GEO_LOCATION: 'Geographic_Location',
    StandardColumnNames.bases: 'Length'
}

fields_not_present_not_null = {
    StandardColumnNames.bio_sample_model,
    StandardColumnNames.center_name,
    StandardColumnNames.experiment,
    StandardColumnNames.instrument,
    StandardColumnNames.platform,
    StandardColumnNames.library_source,
    StandardColumnNames.library_selection,
    StandardColumnNames.library_name,
    StandardColumnNames.library_layout,
    StandardColumnNames.is_retracted,  # bool
    StandardColumnNames.version,
    StandardColumnNames.sample_name,
    StandardColumnNames.sra_study,
    StandardColumnNames.consent_level,
    StandardColumnNames.assay_type,
    StandardColumnNames.bytes,  # int
    StandardColumnNames.datastore_filetype,
    StandardColumnNames.datastore_provider,
    StandardColumnNames.datastore_region
}

fields_not_present_nullable = {
    StandardColumnNames.bio_sample_accession,
    StandardColumnNames.retraction_detected_date,
    StandardColumnNames.serotype,
    StandardColumnNames.avg_spot_length,

}
