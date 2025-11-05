from csv import DictReader
from enum import Enum
from typing import Set

from DB.inserts.amino_acids import find_amino_acid, find_equivalent_amino_acids
from DB.inserts.file_parsers.file_parser import FileParser
from DB.inserts.phenotype_measurement_results import insert_pheno_measurement_result
from DB.inserts.phenotype_metrics import find_or_insert_metric
from DB.models import PhenotypeMetricValues, PhenotypeMetric, AminoAcid
from utils.constants import PhenotypeMetricAssayTypes, DefaultGffFeaturesByRegion
from utils.csv_helpers import get_value, int_from_decimal_str
from utils.errors import NotFoundError


class EveParser(FileParser):
    def __init__(self, filename: str, delimiter: str, gff_feature: str):
        self.filename = filename
        self.delimiter = delimiter
        self.gff_feature = gff_feature

    async def parse_and_insert(self):
        debug_info = {
            'skipped_aas_info_missing': 0,
            'skipped_aas_not_found': 0,
            'count_existing_updated': 0
        }
        cache_phenotype_metrics = dict()
        with open(self.filename, 'r') as f:
            reader = DictReader(f)
            EveParser._verify_header(reader)

            for row in reader:
                # try to get the amino-sub identifying fields
                try:
                    position_aa = get_value(row, ColNameMapping.position_aa.value, transform=int_from_decimal_str)
                    ref_aa = get_value(row, ColNameMapping.ref_aa.value)
                    alt_aa = get_value(row, ColNameMapping.alt_aa.value)
                except ValueError:
                    # If any of these are missing we have to skip the record
                    debug_info['skipped_aas_info_missing'] += 1
                    continue

                try:
                    amino_acid_ids: set[int]  = await find_equivalent_amino_acids(
                        AminoAcid(
                            gff_feature=self.gff_feature,
                            position_aa=position_aa,
                            alt_aa=alt_aa,
                            ref_aa=ref_aa
                        )
                    )

                except NotFoundError:
                    debug_info['skipped_aas_not_found'] += 1
                    # these aren't coming with nt data, I don't want to create a bunch or orphaned amino subs
                    continue

                for col in EveParser._get_data_cols():
                    try:
                        v = get_value(row, col.value, transform=float)
                    except ValueError:
                        continue

                    try:
                        metric_id = cache_phenotype_metrics[col.name]
                    except KeyError:
                        metric_id = await find_or_insert_metric(
                            PhenotypeMetric(
                                name=col.name,
                                assay_type=PhenotypeMetricAssayTypes.EVE
                            )
                        )
                        cache_phenotype_metrics[col.name] = metric_id

                    for aa_id in amino_acid_ids:
                        updated = await insert_pheno_measurement_result(
                            PhenotypeMetricValues(
                                amino_acid_id=aa_id,
                                phenotype_metric_id=metric_id,
                                value=v
                            ),
                            upsert=True
                        )
                        if updated:
                            debug_info['count_existing_updated'] += 1

        print(debug_info)

    @classmethod
    def _verify_header(cls, reader: DictReader) -> None:
        required_columns = cls.get_required_column_set()
        diff = required_columns - set(reader.fieldnames)
        if not len(diff) == 0:
            raise ValueError(f'The following required columns were not found: {diff}')

    @classmethod
    def _get_data_cols(cls):
        return {ColNameMapping.evescape, ColNameMapping.evescape_sigmoid}

    @classmethod
    def get_required_column_set(cls) -> Set[str]:
        cols = {cn.value for cn in {
            ColNameMapping.position_aa,
            ColNameMapping.ref_aa,
            ColNameMapping.alt_aa,
        }}
        for dc in cls._get_data_cols():
            cols.add(dc.value)
        return cols


class ColNameMapping(Enum):
    position_aa = 'i'
    ref_aa = 'wildtype'
    alt_aa = 'mutant'
    evescape = 'evescape'
    evescape_sigmoid = 'evescape_sigmoid'


class EveCsvParser(EveParser):
    def __init__(self, filename: str):
        super().__init__(filename, ',', DefaultGffFeaturesByRegion.HA)
