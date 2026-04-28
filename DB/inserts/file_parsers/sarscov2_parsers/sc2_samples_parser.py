import csv
import sys
import time
from abc import abstractmethod
from time import perf_counter
from typing import Set

import polars as pl

from DB.inserts.file_parsers.file_parser import FileParser
from DB.inserts.geo_locations import find_or_insert_geo_location
from DB.inserts.samples import copy_insert_samples, batch_upsert_samples, get_samples_accession_and_id_as_pl_df, \
    get_samples_accession_id_and_seq_id_as_pl_df
from DB.inserts.sequences import insert_sequences_for_row_numbers
from DB.models import GeoLocation
from utils.constants import StandardColumnNames, COLLECTION_DATE, GEO_LOCATION
from utils.dates_and_times import parse_collection_start_and_end


class Sc2SamplesParser(FileParser):
    def __init__(
        self,
        samples_filename: str,
        unique_seqs_filename: str | None,
        samples_delimiter: str = '\t',
        unique_seqs_delimiter: str = '\t',
        unique_seqs_within_field_delimiter: str = ',',
    ):
        self.samples_filename = samples_filename
        self.samples_delimiter = samples_delimiter
        self._verify_header()

        # todo: we can allow uq seqs file to be none, and treat every sample as unique.
        self.unique_seqs_filename = unique_seqs_filename
        self.unique_seqs_delimiter = unique_seqs_delimiter
        self.unique_seqs_within_field_delimiter = unique_seqs_within_field_delimiter

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
        existing_samples = await get_samples_accession_id_and_seq_id_as_pl_df()

        sequence_id_by_accession = await self._handle_sequences(
            samples_input.select(pl.col(StandardColumnNames.accession)).collect(),
            existing_samples
        )

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
                    fields=[StandardColumnNames.collection_start_date, StandardColumnNames.collection_end_date]
                )
            )
            .unnest(COLLECTION_DATE)
            .join(sequence_id_by_accession.lazy(), on=pl.col(StandardColumnNames.accession), how='left', validate='1:1')
            .collect()
        )
        setup_elapsed = perf_counter() - start
        print(f'samples: starting db ops. setup took {round(setup_elapsed, 2)}s')
        await self._insert_new_samples(samples_finished, existing_samples)
        await self._update_existing_samples(samples_finished, existing_samples)

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

    def _parse_unique_seqs_pl(self) -> pl.DataFrame:
        if self.unique_seqs_filename is None:
            uq = pl.DataFrame(schema={StandardColumnNames.accession: str, 'row_number': int})
        else:
            uq = (
                pl.read_csv(
                    self.unique_seqs_filename,
                    separator=self.unique_seqs_delimiter,
                    row_index_name='row_number',
                    columns=list(self.unique_seqs_accession_columns)
                )
                .with_columns(
                    pl.concat_str(
                        list(self.unique_seqs_accession_columns),
                        separator=self.unique_seqs_within_field_delimiter
                    )
                    .alias('concat_accessions')
                )
                .select(['row_number', 'concat_accessions'])
                .with_columns(pl.col('concat_accessions').str.split(self.unique_seqs_within_field_delimiter))
                .explode('concat_accessions')
                .rename({'concat_accessions': StandardColumnNames.accession})
            )
        return uq

    async def _handle_sequences(
        self,
        samples_input: pl.DataFrame,
        existing_samples: pl.DataFrame
    ) -> pl.DataFrame:
        """
        Read unique sequences input, correlate with existing sequences, add new sequences as needed.
        Return a mapping from accession to sequence id for all input accessions.

        :not a param, but still input uq_seqs_input: accession, row number
        :param samples_input: accession
        :param existing_samples: accession, id, sequence_id
        :return: pl.DataFrame: accession, sequence id --- giving a seq id for every acc listed in samples input
        """

        uq_seqs_input = self._parse_unique_seqs_pl()

        existing_plus_row_number = (
            existing_samples
            .join(uq_seqs_input, on=pl.col(StandardColumnNames.accession), how='left', validate='1:1')
        )

        if not (
                existing_plus_row_number
                        .filter(pl.col('row_number').is_not_null())
                        .select(
                    (pl.col(StandardColumnNames.sequence_id).n_unique().over('row_number') == 1) &
                    (pl.col('row_number').n_unique().over(StandardColumnNames.sequence_id) == 1)
                ).to_series().all()
        ):
            raise ValueError(f'New unique sequence data does not match up with existing. Unable to continue.')

        # Join input samples with uq seqs data.
        # Some samples may not be listed in the uq seqs file. They don't have row numbers
        # We consider these to be un-duplicated samples, and assign each one a unique made up row number.
        max_row_number = uq_seqs_input['row_number'].max()
        if max_row_number is None:
            max_row_number = 0
        samples_input_plus_row_numbers = (
            samples_input
            .join(uq_seqs_input, on=pl.col(StandardColumnNames.accession), how='left', validate='1:1')
            .with_row_index('alt_row_number')
            .with_columns(pl.col('row_number').fill_null(pl.col('alt_row_number') + max_row_number + 1))
            .drop('alt_row_number')
        )

        # Join the row numbers (both from the file and the ones we just made up) into the existing samples data
        # to find seq ids for groups that already exist.
        enriched_row_numbers_plus_existing = samples_input_plus_row_numbers.join(
            existing_samples,
            on=pl.col(StandardColumnNames.accession),
            how="left",
            validate='1:1'
        ).select(pl.col('row_number'), pl.col(StandardColumnNames.sequence_id)).unique()

        # and join back into input samples data. This fills in seq ids for groups that already exist.
        samples_uq_seqs = samples_input_plus_row_numbers.join(
            enriched_row_numbers_plus_existing,
            on=pl.col('row_number'),
            how="left",
            validate='m:1'
        )

        # add new sequences for new groups
        row_numbers_for_new_seqs: list[int] = (
            samples_uq_seqs
            .filter(pl.col(StandardColumnNames.sequence_id).is_null())
            .select(pl.col('row_number'))
            .unique()
            .to_series().to_list()
        )
        new_seq_ids_by_row_number: dict[int, int] = await insert_sequences_for_row_numbers(row_numbers_for_new_seqs)

        # integrate new seq ids
        samples_uq_seqs_complete = (
            samples_uq_seqs
            .with_columns(
                pl.col('row_number')
                .replace_strict(new_seq_ids_by_row_number, default=None)
                .alias('new_sequence_id')
            )
            .with_columns(pl.col(StandardColumnNames.sequence_id).fill_null(pl.col('new_sequence_id')))
            .drop(pl.col('new_sequence_id'))
        )

        # check that no existing samples had their seq ids changed.
        altered_seq_ids = (
            existing_samples
            .join(
                samples_uq_seqs_complete.select(
                    pl.col(StandardColumnNames.accession),
                    pl.col(StandardColumnNames.sequence_id).alias('new_sequence_id')
                ),
                on=pl.col(StandardColumnNames.accession),
                how='inner',
                validate='1:1'
            )
            .filter(pl.col(StandardColumnNames.sequence_id) != pl.col('new_sequence_id'))
        )
        if not altered_seq_ids.is_empty():
            print(altered_seq_ids)
            raise ValueError('An existing sample has had its sequence id changed. This is forbidden.')

        return samples_uq_seqs_complete.select(
            pl.col(StandardColumnNames.accession),
            pl.col(StandardColumnNames.sequence_id)
        )

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


class SC2SDSamplesParser(Sc2SamplesParser):

    def __init__(self, samples_filename: str, unique_sequences_filename: str | None = None):
        super().__init__(samples_filename, unique_sequences_filename)

    def fill_missing_required_cols(self, samples_input: pl.LazyFrame) -> pl.LazyFrame:
        return samples_input.with_columns(
            pl.lit("NA").alias(StandardColumnNames.organism),
            pl.lit(False).alias(StandardColumnNames.is_retracted)
        )

    column_name_map = {
        StandardColumnNames.accession: 'ID',
        StandardColumnNames.host: 'host',
        COLLECTION_DATE: 'collection_date',
        GEO_LOCATION: 'location'
    }

    unique_seqs_accession_columns = {
        'unique_id',
        'dup_ids'
    }


class SC2WastewaterSamplesParser(Sc2SamplesParser):
    def __init__(self, samples_filename: str, unique_sequences_filename: str | None = None):
        super().__init__(samples_filename, unique_sequences_filename)

    def fill_missing_required_cols(self, samples_input: pl.LazyFrame) -> pl.LazyFrame:
        return samples_input.with_columns(
            pl.lit(False).alias(StandardColumnNames.is_retracted)
        )

    column_name_map = {
        StandardColumnNames.accession: 'Accession',
        StandardColumnNames.bio_project: 'Bioprojects',
        StandardColumnNames.bio_sample: 'Biosample',
        StandardColumnNames.host: 'Host_OrganismName',
        StandardColumnNames.isolate: 'Isolate_Name',
        StandardColumnNames.organism: 'Virus_OrganismName',
        StandardColumnNames.isolation_source: 'Isolate_Source',
        COLLECTION_DATE: 'Collection_Date',
        GEO_LOCATION: 'Geographic_Location',
        StandardColumnNames.census_region: 'census_region',
        StandardColumnNames.bases: 'Length',
        StandardColumnNames.ww_viral_load: 'viral_load',
        StandardColumnNames.ww_catchment_population: 'population',
        StandardColumnNames.ww_site_id: 'site_id',
        StandardColumnNames.ww_collected_by: 'collected_by',
    }
