import csv
from typing import Set

from DB.inserts.file_parsers.file_parser import FileParser
from DB.inserts.structural_annotations import insert_structural_annotation
from DB.models import StructuralAnnotation


class StructuralAnnotationsCsvParser(FileParser):

    def __init__(self, filename: str):
        self.filename = filename
        self.delimiter = ','
        self._verify_header()

    async def parse_and_insert(self):
        debug_info = {'count_inserted': 0, 'count_skipped': 0}

        with open(self.filename, 'r') as f:
            reader = csv.DictReader(f, delimiter=self.delimiter)
            for row in reader:
                try:
                    sequential_site = row['sequential_site']
                    note_parts = [
                        f"reference_site={row['reference_site']}",
                        f"reference_H1_site={row['reference_H1_site']}",
                        f"mature_H5_site={row['mature_H5_site']}",
                        f"HA1_HA2_H5_site={row['HA1_HA2_H5_site']}",
                        f"region={row['region']}",
                    ]
                    structural_note = '; '.join(note_parts)

                    await insert_structural_annotation(
                        StructuralAnnotation(
                            sequential_site=sequential_site,
                            structural_note=structural_note
                        )
                    )
                    debug_info['count_inserted'] += 1
                except (KeyError, ValueError):
                    debug_info['count_skipped'] += 1
                    continue
        print(debug_info)

    @classmethod
    def get_required_column_set(cls) -> Set[str]:
        return {
            'sequential_site', 'reference_site', 'reference_H1_site',
            'mature_H5_site', 'HA1_HA2_H5_site', 'region'
        }

    def _verify_header(self):
        with open(self.filename, 'r') as f:
            reader = csv.DictReader(f, delimiter=self.delimiter)
            required = self.get_required_column_set()
            diff = required - set(reader.fieldnames)
            if len(diff) != 0:
                raise ValueError(f'Missing required columns: {diff}')
