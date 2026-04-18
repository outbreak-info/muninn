import csv
import sys
from typing import Set

from DB.inserts.file_parsers.file_parser import FileParser
from DB.inserts.samples import get_sample_ids_by_accessions
from utils.csv_helpers import get_value


class UniqueSequencesParser(FileParser):
    """
    This parser will assume that each line of a file describes a set of samples with duplicate sequences.
    All samples given on each line will be linked to a single sequence.
    """
    def __init__(self, filename: str):
        self.filename = filename
        self.delimiter = '\t'
        self.dups_delimiter = ','

        self._verify_header()

    async def parse_and_insert(self):
        debug_info = {
            'accessions_not_present': set()
        }

        csv.field_size_limit(int(sys.maxsize/100))
        with open(self.filename, 'r') as f:
            reader = csv.DictReader(f, delimiter=self.delimiter)
            for row in reader:
                accessions = {get_value(row, 'unique_id')}
                accessions.update(get_value(row, 'dup_ids', transform=lambda s: set(s.split(self.dups_delimiter))))

                # todo: toss any accessions not already in samples
                ids_by_accession = await get_sample_ids_by_accessions(list(accessions))
                if len(ids_by_accession) < len(accessions):
                    missing = accessions - set(ids_by_accession.keys())
                    debug_info['accessions_not_present'].update(missing)

                # todo: make sure that none of these accessions are already associated with a sequence

                # todo: create a new sequence

                # todo: associate samples with sequence

                print(accessions)


    def _verify_header(self):
        with open(self.filename, 'r') as f:
            reader = csv.DictReader(f, delimiter=self.delimiter)
            diff = self.required_columns - set(reader.fieldnames)
            if not len(diff) == 0:
                raise ValueError(f'The following required columns were not found: {diff}')

    @classmethod
    def get_required_column_set(cls) -> Set[str]:
        return cls.required_columns

    # Columns containing sample accessions
    # All samples listed on each line will be linked together to a single sequence
    required_columns = {
        'unique_id',
        'dup_ids'
    }