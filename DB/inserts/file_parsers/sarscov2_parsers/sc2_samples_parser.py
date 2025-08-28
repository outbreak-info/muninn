import csv
import time
from time import perf_counter
from typing import Set, Any, Dict

import polars as pl

from DB.inserts.file_parsers.file_parser import FileParser
from DB.inserts.geo_locations import find_or_insert_geo_location
from DB.inserts.samples import copy_insert_samples, batch_upsert_samples
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
        start = perf_counter()
        # Scan file, rename columns, drop unused cols, drop rows with null collection date
        samples_input = (
            pl.scan_csv(self.filename, separator=self.delimiter)
            .rename({old: new for new, old in column_name_map.items()})
            .select(set(column_name_map.keys()))
            .drop_nulls([pl.col(COLLECTION_DATE)])
            .with_columns(  # These columns are not used in the data, but are required by the schema
                pl.col(StandardColumnNames.bio_project).fill_null('NA'),
                pl.col(StandardColumnNames.release_date).str.to_datetime(format="%Y-%m-%d", strict=False),
                pl.col(StandardColumnNames.creation_date).str.to_datetime(format="%Y-%m-%d", strict=False),
            )
        )
        # unique by accession? No, leave it out for now to force errors on conflict.

        # Add in not-null columns with placeholder values
        samples_padded = (
            samples_input
            .join(SC2SamplesParser._get_placeholder_value_map(), how='cross')
        )

        geo_locations = await SC2SamplesParser._insert_geo_locations(samples_input)
        existing_samples = await get_samples_accession_and_id_as_pl_df()

        samples_finished: pl.DataFrame = (
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
            .collect(engine='streaming')
        )
        setup_elapsed = perf_counter() - start
        print(f'samples: starting db ops. setup took {round(setup_elapsed, 2)}s')
        await SC2SamplesParser._insert_new_samples(samples_finished, existing_samples)
        await SC2SamplesParser._update_existing_samples(samples_finished, existing_samples)

    @staticmethod
    async def _insert_geo_locations(samples_input: pl.LazyFrame) -> pl.DataFrame:
        """
        Insert geo_locations from samples, return original geo_location strings and db ids
        :param samples_input:
        :return: geo_location <str>, id <int> to be joined with samples
        """
        # this is still done the slow way, it doesn't take long enough to be worth updating yet
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
    async def _insert_new_samples(samples_finished: pl.DataFrame, existing_samples: pl.DataFrame):
        new_samples = samples_finished.join(
            existing_samples,
            on=pl.col(StandardColumnNames.accession),
            how='anti'
        )
        copy_status = await copy_insert_samples(new_samples)
        print(f'new samples: {copy_status}')

    @staticmethod
    async def _update_existing_samples(samples_finished: pl.DataFrame, existing_samples: pl.DataFrame):
        updated_samples = samples_finished.join(
            existing_samples,
            on=pl.col(StandardColumnNames.accession),
            how='inner'
        )
        await batch_upsert_samples(updated_samples)

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
        return set(column_name_map.keys())


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
    StandardColumnNames.creation_date: 'UpdateDate',  # todo: check on this mapping
    GEO_LOCATION: 'Geographic_Location',
    StandardColumnNames.bases: 'Length',
    StandardColumnNames.ww_viral_load: 'viral_load',
    StandardColumnNames.ww_catchment_population : 'population',
    StandardColumnNames.ww_site_id: 'site_id',
    StandardColumnNames.ww_collected_by: 'collected_by',
    StandardColumnNames.ww_epiweek: 'epiweek',
    StandardColumnNames.ww_census_region: 'census_region'
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
