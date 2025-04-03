import csv
from csv import DictReader
from enum import Enum

from DB.inserts.amino_acid_substitutions import find_aa_sub
from DB.inserts.file_formats.file_format import FileFormat
from DB.inserts.phenotype_measurement_results import insert_pheno_measurement_result
from DB.inserts.phenotype_metrics import find_or_insert_metric
from DB.models import AminoAcidSubstitution, PhenotypeMetric, PhenotypeMeasurementResult
from utils.csv_helpers import get_value, int_from_decimal_str
from utils.errors import NotFoundError


class EveDmsCsv(FileFormat):

    @classmethod
    async def insert_from_file(cls, filename: str) -> None:
        debug_info = {
            'skipped_aas_info_missing': 0,
            'skipped_aas_not_found': 0,
            'value_conflicts': 0
        }
        cache_phenotype_metrics = dict()
        with open(filename, 'r') as f:
            reader = csv.DictReader(f)
            cls._verify_header(reader)

            for row in reader:
                # try to get the amino-sub identifying fields
                try:
                    position_aa = get_value(row, cls.ColNameMapping.position_aa.value, transform=int_from_decimal_str)
                    ref_aa = get_value(row, cls.ColNameMapping.ref_aa.value)
                    alt_aa = get_value(row, cls.ColNameMapping.alt_aa.value)
                except ValueError:
                    # If any of these are missing we have to skip the record
                    debug_info['skipped_aas_info_missing'] += 1
                    continue

                # todo
                gff_feature = 'HA:cds-XAJ25415.1'

                try:
                    aas_id = await find_aa_sub(
                        AminoAcidSubstitution(
                            gff_feature=gff_feature,
                            position_aa=position_aa,
                            alt_aa=alt_aa,
                            ref_aa=ref_aa
                        )
                    )
                except NotFoundError:
                    debug_info['skipped_aas_not_found'] += 1
                    # these aren't coming with nt data, I don't want to create a bunch or orphaned amino subs
                    continue

                # todo: these need to be standardized
                assay_type = 'EVEscape'

                data_cols = {cls.ColNameMapping.evescape, cls.ColNameMapping.evescape_sigmoid}
                for col in data_cols:
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
                                assay_type=assay_type
                            )
                        )
                        cache_phenotype_metrics[col.name] = metric_id
                    try:
                        await insert_pheno_measurement_result(
                            PhenotypeMeasurementResult(
                                amino_acid_substitution_id=aas_id,
                                phenotype_metric_id=metric_id,
                                value=v
                            )
                        )
                    except ValueError:
                        debug_info['value_conflicts'] += 1

        print(debug_info)


    class ColNameMapping(Enum):
        position_aa = 'i'
        ref_aa = 'wildtype'
        alt_aa = 'mutant'
        evescape = 'evescape'
        evescape_sigmoid = 'evescape_sigmoid'

    @classmethod
    def _verify_header(cls, reader: DictReader) -> None:
        required_cols = {cn.value for cn in {
            cls.ColNameMapping.position_aa,
            cls.ColNameMapping.ref_aa,
            cls.ColNameMapping.alt_aa,
            cls.ColNameMapping.evescape,
            cls.ColNameMapping.evescape_sigmoid
        }}

        if not required_cols <= set(reader.fieldnames):
            raise ValueError('did not find all required fields in header')
