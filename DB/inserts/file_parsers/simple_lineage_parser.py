import csv
import re
from csv import DictReader
from typing import Set

from sqlalchemy.exc import IntegrityError

from DB.inserts.file_parsers.file_parser import FileParser
from DB.inserts.lineage_systems import find_or_insert_lineage_system
from DB.inserts.lineages import find_or_insert_lineage
from DB.inserts.samples import get_sample_id_by_accession
from DB.inserts.samples_lineages import insert_sample_lineage
from DB.models import LineageSystem, Lineage, SampleLineage
from utils.constants import StandardColumnNames, StandardLineageSystemNames
from utils.csv_helpers import get_value
from utils.errors import NotFoundError


class SimpleLineageParser(FileParser):

    def __init__(self, filename: str, delimiter: str, lineage_system_name: str):
        self.filename = filename
        self.lineage_system_name = lineage_system_name
        self.delimiter = delimiter

    async def parse_and_insert(self):
        debug_info = {
            'skipped_malformed': 0,
            'skipped_sample_not_found': 0,
            'skipped_duplicate_for_sample': 0,
            'genotype_not_assigned': 0,
            'integrity_errors': 0
        }

        accessions_seen = set()

        # we only use a single lineage system, so just store it
        lineage_system_id = await find_or_insert_lineage_system(
            LineageSystem(
                lineage_system_name=self.lineage_system_name
            )
        )

        # since we always have the same system id, we only need: name -> id
        cache_lineage_ids = dict()

        with open(self.filename, 'r') as f:
            reader = csv.DictReader(f, delimiter=self.delimiter)
            self._verify_header(reader)

            for row in reader:
                try:
                    sample_accession = get_value(row, self.column_name_map[StandardColumnNames.accession])
                    genotype = get_value(row, self.column_name_map[StandardColumnNames.lineage_name])
                except ValueError:
                    debug_info['skipped_malformed'] += 1
                    continue

                if re.match(r'not assigned', genotype, re.IGNORECASE):
                    debug_info['genotype_not_assigned'] += 1
                    continue

                # find sample
                if sample_accession in accessions_seen:
                    debug_info['skipped_duplicate_for_sample'] += 1
                    continue
                try:
                    sample_id = await get_sample_id_by_accession(sample_accession)
                except NotFoundError:
                    debug_info['skipped_sample_not_found'] += 1
                    continue

                # find lineage
                try:
                    lineage_id = cache_lineage_ids[genotype]
                except KeyError:
                    lineage_id = await find_or_insert_lineage(
                        Lineage(
                            lineage_name=genotype,
                            lineage_system_id=lineage_system_id
                        )
                    )
                    cache_lineage_ids[genotype] = lineage_id

                # insert sample-lineage
                try:
                    await insert_sample_lineage(
                        SampleLineage(
                            sample_id=sample_id,
                            lineage_id=lineage_id,
                            abundance=None,
                            is_consensus_call=True
                        )
                    )
                except IntegrityError:
                    debug_info['integrity_errors'] += 1

                accessions_seen.add(sample_accession)

        print(debug_info)

    @classmethod
    def _verify_header(cls, reader: DictReader) -> None:
        required_cols = cls.get_required_column_set()
        diff = required_cols - set(reader.fieldnames)
        if not len(diff) == 0:
            raise ValueError(f'The following required columns were not found: {diff}')

    @classmethod
    def get_required_column_set(cls) -> Set[str]:
        return {v for v in cls.column_name_map.values()}

    column_name_map = {
        StandardColumnNames.lineage_name: StandardColumnNames.lineage_name,
        StandardColumnNames.accession: StandardColumnNames.accession
    }


class GenofluLineageParser(SimpleLineageParser):
    def __init__(self, filename: str):
        super().__init__(filename, '\t', StandardLineageSystemNames.genoflu)

    async def parse_and_insert(self):
        await super().parse_and_insert()

    column_name_map = {
        StandardColumnNames.lineage_name: 'Genotype',
        StandardColumnNames.accession: 'sample'
    }


class Sc2LineageParser(SimpleLineageParser):
    def __init__(self, filename: str):
        super().__init__(filename, ',', 'PANGO')

    async def parse_and_insert(self):
        await super().parse_and_insert()

    column_name_map = {
        StandardColumnNames.accession: 'taxon',
        StandardColumnNames.lineage_name: 'lineage'
    }