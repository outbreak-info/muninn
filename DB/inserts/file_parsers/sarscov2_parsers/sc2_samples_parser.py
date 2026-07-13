import csv
import time
from abc import abstractmethod
from time import perf_counter
from typing import Set

import polars as pl

from DB.inserts.file_parsers.file_parser import FileParser
from DB.inserts.geo_locations import find_or_insert_geo_location
from DB.inserts.samples import copy_insert_samples, batch_upsert_samples, get_samples_accession_and_id_as_pl_df
from DB.models import GeoLocation
from utils.constants import ColumnNames, COLLECTION_DATE, GEO_LOCATION
from utils.dates_and_times import parse_collection_start_and_end


class Sc2SamplesParser(FileParser):
    def __init__(
        self,
        samples_filename: str,
        unique_seqs_filename: str | None,
        samples_delimiter: str = '\t',
        unique_seqs_delimiter: str = '\t',
        unique_seqs_within_field_delimiter: str = ',',
        geo_location_levels_delimiter: str = '/'
    ):
        self.samples_filename = samples_filename
        self.samples_delimiter = samples_delimiter
        self._verify_header()

        self.unique_seqs_filename = unique_seqs_filename
        self.unique_seqs_delimiter = unique_seqs_delimiter
        self.unique_seqs_within_field_delimiter = unique_seqs_within_field_delimiter
        self.geo_location_levels_delimiter = geo_location_levels_delimiter

    async def parse_and_insert(self):
        start = perf_counter()
        # Scan file, rename columns, drop unused cols, drop rows with null collection date
        samples_input = (
            pl.scan_csv(self.samples_filename, separator=self.samples_delimiter)
            .rename({old: new for new, old in self.column_name_map.items()})
            .select(set(self.column_name_map.keys()))
            .drop_nulls([pl.col(COLLECTION_DATE)])
        )
        samples_input = self.fill_missing_required_cols(samples_input)
        # unique by accession? No, leave it out for now to force errors on conflict.

        geo_locations = await self._insert_geo_locations(samples_input)
        existing_samples = await get_samples_accession_and_id_as_pl_df()

        samples_finished: pl.DataFrame = (
            samples_input
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
                    fields=[ColumnNames.collection_start_date, ColumnNames.collection_end_date]
                )
            )
            .unnest(COLLECTION_DATE)
            .collect()
        )
        setup_elapsed = perf_counter() - start
        print(f'samples: starting db ops. setup took {round(setup_elapsed, 2)}s')
        await self._insert_new_samples(samples_finished, existing_samples)
        await self._update_existing_samples(samples_finished, existing_samples)

    async def _insert_geo_locations(self, samples_input: pl.LazyFrame) -> pl.DataFrame:
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
                pl.col(GEO_LOCATION).str.split(self.geo_location_levels_delimiter).list.to_struct(
                    n_field_strategy="max_width",
                    fields=[
                        ColumnNames.country_name,
                        ColumnNames.admin1_name,
                        ColumnNames.admin2_name,
                        ColumnNames.admin3_name
                    ]
                )
                .alias('tmp_geo_struct')
            )
            .unnest('tmp_geo_struct')
            .with_columns(
                [
                    pl.col(ColumnNames.country_name).str.strip_chars(),
                    pl.col(ColumnNames.admin1_name).str.strip_chars(),
                    pl.col(ColumnNames.admin2_name).str.strip_chars(),
                    pl.col(ColumnNames.admin3_name).str.strip_chars(),
                ]
            )
            .collect()
        )

        ids = []
        for row in geo_locations.iter_rows(named=True):
            ids.append(
                await find_or_insert_geo_location(
                    GeoLocation(
                        country_name=row[ColumnNames.country_name],
                        admin1_name=row[ColumnNames.admin1_name],
                        admin2_name=row[ColumnNames.admin2_name],
                        admin3_name=row[ColumnNames.admin3_name]
                    )
                )
            )

        geo_locations = (
            geo_locations
            .select(pl.col(GEO_LOCATION))
            .with_columns(pl.Series(ids).alias(ColumnNames.geo_location_id))
        )
        print(f'geo locations took {round(time.perf_counter() - start, 2)}s')
        return geo_locations

    @staticmethod
    async def _insert_new_samples(samples_finished: pl.DataFrame, existing_samples: pl.DataFrame):
        new_samples = samples_finished.join(
            existing_samples,
            on=pl.col(ColumnNames.accession),
            how='anti'
        )
        copy_status = await copy_insert_samples(new_samples)
        print(f'new samples: {copy_status}')

    @staticmethod
    async def _update_existing_samples(samples_finished: pl.DataFrame, existing_samples: pl.DataFrame):
        updated_samples = samples_finished.join(
            existing_samples,
            on=pl.col(ColumnNames.accession),
            how='inner'
        )
        await batch_upsert_samples(updated_samples)

    def _verify_header(self):
        with open(self.samples_filename, 'r') as f:
            reader = csv.DictReader(f, delimiter=self.samples_delimiter)
            required_columns = set(self.column_name_map.values())
            if not set(reader.fieldnames) >= required_columns:
                raise ValueError(f'Missing required fields: {required_columns - set(reader.fieldnames)}')

    @classmethod
    def get_required_column_set(cls) -> Set[str]:
        return set(cls.column_name_map.keys())

    @abstractmethod
    def fill_missing_required_cols(self, samples_input: pl.LazyFrame) -> pl.LazyFrame:
        raise NotImplementedError

    column_name_map = dict()

    unique_seqs_accession_columns = set()


class Sc2SdSamplesParser(Sc2SamplesParser):

    def __init__(self, samples_filename: str, unique_sequences_filename: str | None = None):
        super().__init__(samples_filename, unique_sequences_filename)

    def fill_missing_required_cols(self, samples_input: pl.LazyFrame) -> pl.LazyFrame:
        return samples_input.with_columns(
            pl.lit("NA").alias(ColumnNames.organism),
            pl.lit(False).alias(ColumnNames.is_retracted)
        )

    column_name_map = {
        ColumnNames.accession: 'ID',
        ColumnNames.host: 'host',
        COLLECTION_DATE: 'collection_date',
        GEO_LOCATION: 'location'
    }

    unique_seqs_accession_columns = {
        'unique_id',
        'dup_ids'
    }


class Sc2WastewaterSamplesParser(Sc2SamplesParser):
    def __init__(self, samples_filename: str, unique_sequences_filename: str | None = None):
        super().__init__(samples_filename, unique_sequences_filename)

    def fill_missing_required_cols(self, samples_input: pl.LazyFrame) -> pl.LazyFrame:
        return samples_input.with_columns(
            pl.lit(False).alias(ColumnNames.is_retracted)
        )

    column_name_map = {
        ColumnNames.accession: 'Accession',
        ColumnNames.bio_project: 'Bioprojects',
        ColumnNames.bio_sample: 'Biosample',
        ColumnNames.host: 'Host_OrganismName',
        ColumnNames.isolate: 'Isolate_Name',
        ColumnNames.organism: 'Virus_OrganismName',
        ColumnNames.isolation_source: 'Isolate_Source',
        COLLECTION_DATE: 'Collection_Date',
        GEO_LOCATION: 'Geographic_Location',
        ColumnNames.census_region: 'census_region',
        ColumnNames.bases: 'Length',
        ColumnNames.ww_viral_load: 'viral_load',
        ColumnNames.ww_catchment_population: 'population',
        ColumnNames.ww_site_id: 'site_id',
        ColumnNames.ww_collected_by: 'collected_by',
    }


class Sc2NcbiSamplesParser(Sc2SamplesParser):
    def __init__(self, samples_filename: str, unique_sequences_filename: str | None = None):
        super().__init__(samples_filename, unique_sequences_filename, geo_location_levels_delimiter=':')

    def fill_missing_required_cols(self, samples_input: pl.LazyFrame) -> pl.LazyFrame:
        return samples_input.with_columns(
            pl.lit("NA").alias(ColumnNames.organism),
            pl.lit(False).alias(ColumnNames.is_retracted)
        )

    column_name_map = {
        ColumnNames.accession: 'Accession',
        ColumnNames.bio_project: 'Bioprojects',
        ColumnNames.bio_sample: 'Biosample',
        ColumnNames.host: 'Host_OrganismName',
        ColumnNames.isolate: 'Isolate_Name',
        ColumnNames.organism: 'Virus_OrganismName',
        ColumnNames.isolation_source: 'Isolate_Source',
        COLLECTION_DATE: 'Collection_Date',
        GEO_LOCATION: 'Geographic_Location',
        ColumnNames.census_region: 'census_region',
        ColumnNames.bases: 'Length',
        ColumnNames.ww_viral_load: 'viral_load',
        ColumnNames.ww_catchment_population: 'population',
        ColumnNames.ww_site_id: 'site_id',
        ColumnNames.ww_collected_by: 'collected_by',
    }

    unique_seqs_accession_columns = {
        'unique_id',
        'dup_ids'
    }
