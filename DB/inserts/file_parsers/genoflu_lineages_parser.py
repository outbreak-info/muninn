import csv
import re
from csv import DictReader
from enum import Enum


from sqlalchemy.exc import IntegrityError

from DB.inserts.file_parsers.file_parser import FileParser
from DB.inserts.lineage_systems import find_or_insert_lineage_system
from DB.inserts.lineages import find_or_insert_lineage
from DB.inserts.samples import find_sample_id_by_accession
from DB.inserts.samples_lineages import insert_sample_lineage
from DB.models import LineageSystem, Lineage, SampleLineage
from utils.csv_helpers import get_value
from utils.errors import NotFoundError


# this is a trial for a different way of setting up the parser / ingestion files.
# using class methods for everything feels like I'm working against the grain.
# I think that using objects will make it a bit easier.
# it'll allow me to have more setup logic and make it more natural to reuse the classes on multiple input files.

class GenofluLineagesParser(FileParser):
    lineage_system_name = 'usda_genoflu'

    def __init__(self, filename: str):
        self.filename = filename

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
                lineage_system_name=GenofluLineagesParser.lineage_system_name
            )
        )

        # since we always have the same system id, we only need: name -> id
        cache_lineage_ids = dict()

        with open(self.filename, 'r') as f:
            reader = csv.DictReader(f, delimiter='\t')
            GenofluLineagesParser._verify_header(reader)

            for row in reader:
                try:
                    sample_accession = get_value(row, GenofluLineagesParser.ColNameMapping.sample_accession.value)
                    genotype = get_value(row, GenofluLineagesParser.ColNameMapping.genotype.value)
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
                    sample_id = await find_sample_id_by_accession(sample_accession)
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

        #todo: logging
        print(debug_info)


    @classmethod
    def _verify_header(cls, reader: DictReader) -> None:
        required_cols = {cn.value for cn in {
            cls.ColNameMapping.sample_accession,
            cls.ColNameMapping.genotype
        }}

        if not required_cols <= set(reader.fieldnames):
            raise ValueError('did not find all required fields in header')

    class ColNameMapping(Enum):
        sample_accession = 'sample'
        genotype = 'Genotype'
