import re
from glob import glob
from os import path
from typing import List

from psycopg2 import IntegrityError

from DB.inserts.file_parsers.file_parser import FileParser
from DB.inserts.lineage_systems import find_or_insert_lineage_system
from DB.inserts.lineages import find_or_insert_lineage
from DB.inserts.samples import find_sample_id_by_accession
from DB.inserts.samples_lineages import upsert_sample_lineage
from DB.models import LineageSystem, Lineage, SampleLineage
from utils.constants import LineageSystemNames
from utils.errors import NotFoundError

LINEAGES = 'lineages'
ABUNDANCES = 'abundances'


class FreyjaDemixedParser(FileParser):

    def __init__(self, target_dir: str):
        if not path.isdir(target_dir):
            raise ValueError('FreyjaDemixedParser must be provided with a directory as a target')
        self.target_dir = target_dir
        self.file_by_accession = self._list_files_by_accession()

    def _list_files_by_accession(self) -> dict:
        file_by_accession = dict()
        demixed_files = glob(path.join(self.target_dir, '*.demixed'))
        for df in demixed_files:
            basename = path.basename(df)
            accession = basename.removesuffix('.demixed')
            if accession in file_by_accession.keys():
                print(f"Duplicate demixed file for {accession}. This shouldn't even be possible!")
                continue
            file_by_accession[accession] = df
        return file_by_accession

    async def parse_and_insert(self):
        debug_info = {
            'skipped_malformed': 0,
            'skipped_sample_not_found': 0,
            'count_existing_records_modified': 0,
            'count_integrity_errors': 0,
        }
        lineage_system_id = await find_or_insert_lineage_system(
            LineageSystem(
                lineage_system_name=LineageSystemNames.freyja_demixed
            )
        )
        # format = name -> id
        cache_lineage_ids = dict()
        for accession, file in self.file_by_accession.items():
            # parse data from file
            try:
                abundance_by_lineage = FreyjaDemixedParser._parse_file(accession, file)
            except ValueError as e:
                print(e)
                debug_info['skipped_malformed'] += 1
                continue
            # find sample
            try:
                sample_id = await find_sample_id_by_accession(accession)
            except NotFoundError:
                debug_info['skipped_sample_not_found'] += 1
                continue

            for lineage_name, abundance in abundance_by_lineage.items():
                # get lineage id
                try:
                    lineage_id = cache_lineage_ids[lineage_name]
                except KeyError:
                    lineage_id = await find_or_insert_lineage(
                        Lineage(
                            lineage_name=lineage_name,
                            lineage_system_id=lineage_system_id
                        )
                    )
                    cache_lineage_ids[lineage_name] = lineage_id
                # insert sample-lineage data
                try:
                    modified = await upsert_sample_lineage(
                        SampleLineage(
                            lineage_id=lineage_id,
                            sample_id=sample_id,
                            is_consensus_call=False,
                            abundance=abundance
                        )
                    )
                    if modified:
                        debug_info['count_existing_records_modified'] += 1
                except IntegrityError:
                    debug_info['count_integrity_errors'] += 1
                    continue
        print(debug_info)

    @classmethod
    def _parse_file(cls, accession: str, file: str) -> dict[str, float]:
        """
        File Format:

            _\t_ SRR29281430_variants.tsv
            summarized _\t_ [('Other', 0.9982764565357015)]
            lineages _\t_ H5Nx-A.1 H5Nx-A.1.8 H5Nx-A.1.7 H5Nx-A.1.16 H5Nx-A.1.10 H5Nx-A.1.3 H5Nx-A.1.11 H5Nx-A.1.4.3 H5Nx-A.1.3.2 H5Nx-A.1.4.2 H5Nx-A.1.12
            abundances _\t_ 0.15832359 0.15729060 0.15729060 0.15729059 0.15729059 0.15598680 0.04545500 0.00330000 0.00294118 0.00159236 0.00151515
            resid _\t_ 12.519881748345714
            coverage _\t_ 82.55681818181819

        :return:
        """
        lineages = abundances = None
        with open(file, 'r') as f:
            for line in f.readlines():
                line = line.strip()
                if line.startswith(LINEAGES):
                    lineages = FreyjaDemixedParser._parse_lineages_line(line)
                elif line.startswith(ABUNDANCES):
                    abundances = FreyjaDemixedParser._parse_abundances_line(line)
                elif '_variants.tsv' in line:
                    accession_2 = line.removesuffix('_variants.tsv').strip()
                    if accession != accession_2:
                        raise ValueError(f'Accession within file does not match filename: {file}')
                else:
                    continue

            if lineages is None:
                raise ValueError(f'Lineages not found in file: {file}')

            if abundances is None:
                raise ValueError(f'Abundances not found in file: {file}')

            if len(lineages) != len(abundances):
                raise ValueError(f'lineages and abundances have mismatched lengths: {file}')

            return dict(zip(lineages, abundances))

    @classmethod
    def _parse_lineages_line(cls, line: str) -> List[str]:
        wo_prefix = line.removeprefix(LINEAGES).strip()
        return re.split(r'\s+', wo_prefix)

    @classmethod
    def _parse_abundances_line(cls, line: str) -> List[float]:
        wo_prefix = line.removeprefix(ABUNDANCES).strip()
        return [float(a) for a in re.split(r'\s+', wo_prefix)]

    @classmethod
    def get_required_column_set(cls):
        return {'not really applicable, requires abundances line and lineages lines'}
