import csv
from enum import Enum
from typing import Set, Any, Dict

from DB.inserts.file_parsers.file_parser import FileParser
from DB.inserts.samples import copy_insert_samples
from utils.constants import StandardColumnNames, COLLECTION_DATE, GEO_LOCATION
import polars as pl

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
        # todo: unique?

        # todo: Geo Locations

        geo_locations = (
            samples_input
            .select(pl.col(GEO_LOCATION))
            .unique()
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
            .with_columns_seq(

            )
        )

        # Add in not-null columns with placeholder values
        samples_padded = samples_input.join(SC2SamplesParser._get_placeholder_value_map(), how='cross')

        samples_to_insert = (
            samples_padded
            .drop(pl.col(GEO_LOCATION))
            .with_columns(pl.col(COLLECTION_DATE).map_elements(
                parse_collection_start_and_end,
                return_dtype=pl.List(pl.Date)
            ))
            .with_columns(
                pl.col(COLLECTION_DATE).list.to_struct(
                    fields=[StandardColumnNames.collection_start_date, StandardColumnNames.collection_end_date]
                )
            )
            .unnest(COLLECTION_DATE)
        )

        copy_status = await copy_insert_samples(samples_to_insert.collect())
        print(f'samples: {copy_status}')

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
