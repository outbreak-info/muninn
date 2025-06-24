from csv import DictReader
from typing import Set, Dict

from DB.inserts.amino_acid_substitutions import find_aa_sub
from DB.inserts.file_parsers.file_parser import FileParser
from DB.inserts.phenotype_measurement_results import insert_pheno_measurement_result
from DB.inserts.phenotype_metrics import find_or_insert_metric
from DB.models import AminoAcidSubstitution, PhenotypeMetric, PhenotypeMeasurementResult
from utils.constants import PhenotypeMetricAssayTypes, DefaultGffFeaturesByRegion, StandardColumnNames, \
    StandardPhenoMetricNames
from utils.csv_helpers import get_value
from utils.errors import NotFoundError


class DmsFileParser(FileParser):

    def __init__(self, filename: str, delimiter: str, gff_feature: str):
        self.filename = filename
        self.delimiter = delimiter
        self.gff_feature = gff_feature

    async def parse_and_insert(self):
        debug_info = {
            'skipped_aas_data_missing': 0,
            'skipped_aas_not_found': 0,
            'value_parsing_errors': 0,
            'count_existing_updated': 0  # only counts if the value changed
        }
        # format = metric_name -> id
        cache_metric_ids = dict()
        # format = (position_aa, ref_aa, alt_aa) -> id
        cache_amino_sub_ids = dict()
        # format = (position_aa, ref_aa, alt_aa)
        cache_amino_subs_not_found = set()
        with open(self.filename, 'r') as f:
            reader = DictReader(f, delimiter=self.delimiter)
            self._verify_header(reader)
            present_data_cols = DmsFileParser._get_present_data_columns(reader)

            for row in reader:
                try:
                    position_aa = get_value(
                        row,
                        required_column_name_map[StandardColumnNames.position_aa],
                        transform=int
                    )
                    ref_aa = get_value(row, required_column_name_map[StandardColumnNames.ref_aa])
                    alt_aa = get_value(row, required_column_name_map[StandardColumnNames.alt_aa])
                except ValueError:
                    debug_info['skipped_aas_info_missing'] += 1
                    continue

                if (position_aa, ref_aa, alt_aa) in cache_amino_subs_not_found:
                    debug_info['skipped_aas_not_found'] += 1
                    continue
                try:
                    aas_id = cache_amino_sub_ids[(position_aa, ref_aa, alt_aa)]
                except KeyError:
                    try:
                        aas_id = await find_aa_sub(
                            AminoAcidSubstitution(
                                gff_feature=self.gff_feature,
                                position_aa=position_aa,
                                alt_aa=alt_aa,
                                ref_aa=ref_aa
                            )
                        )
                        cache_amino_sub_ids[(position_aa, ref_aa, alt_aa)] = aas_id
                    except NotFoundError:
                        # if the aas doesn't already exist, skip the record.
                        # we don't want to create orphaned aas entries just for the dms data
                        debug_info['skipped_aas_not_found'] += 1
                        cache_amino_subs_not_found.add((position_aa, ref_aa, alt_aa))
                        continue

                for canonical_name, input_name in present_data_cols.items():
                    try:
                        v = get_value(row, input_name, transform=float)
                    except ValueError:
                        debug_info['value_parsing_errors'] += 1
                        continue

                    try:
                        metric_id = cache_metric_ids[canonical_name]
                    except KeyError:
                        metric_id = await find_or_insert_metric(
                            PhenotypeMetric(
                                name=canonical_name,
                                assay_type=PhenotypeMetricAssayTypes.DMS
                            )
                        )
                        cache_metric_ids[canonical_name] = metric_id

                    updated = await insert_pheno_measurement_result(
                        PhenotypeMeasurementResult(
                            amino_acid_substitution_id=aas_id,
                            phenotype_metric_id=metric_id,
                            value=v
                        ),
                        upsert=True
                    )
                    if updated:
                        debug_info['count_existing_updated'] += 1

        debug_info['count_aas_not_found'] = len(cache_amino_subs_not_found)
        print(debug_info)


    @staticmethod
    def _get_present_data_columns(reader: DictReader) -> Dict[str, str]:
        actual_cols = set(reader.fieldnames)

        data_cols_present = {k: v for k, v in data_column_name_map.items() if v in actual_cols}
        if len(data_cols_present) == 0:
            raise ValueError(
                f'No DMS data columns found, so no values can be extracted from this file. '
                f'Available data columns: {set(data_column_name_map.values())}'
            )
        return data_cols_present

    @classmethod
    def _verify_header(cls, reader: DictReader):
        required_cols = cls.get_required_column_set()
        actual_cols = set(reader.fieldnames)
        diff = required_cols - actual_cols
        if not len(diff) == 0:
            raise ValueError(f'Not all required columns are present, missing: {diff}')

    @classmethod
    def get_required_column_set(cls) -> Set[str]:
        return set(required_column_name_map.values())


required_column_name_map = {
    StandardColumnNames.position_aa: 'sequential_site',
    StandardColumnNames.ref_aa: 'wildtype',
    StandardColumnNames.alt_aa: 'mutant',
}
data_column_name_map = {
    StandardPhenoMetricNames.species_sera_escape: 'species sera escape',
    StandardPhenoMetricNames.entry_in_293t_cells: 'entry in 293T cells',
    StandardPhenoMetricNames.stability: 'stability',
    StandardPhenoMetricNames.sa26_usage_increase: 'SA26 usage increase',
    StandardPhenoMetricNames.mature_h5_site: 'mature_H5_site',
    StandardPhenoMetricNames.ferret_sera_escape: 'ferret sera escape',
    StandardPhenoMetricNames.mouse_sera_escape: 'mouse sera escape',
}


class HaRegionDmsTsvParser(DmsFileParser):
    def __init__(self, filename: str):
        super().__init__(filename, '\t', DefaultGffFeaturesByRegion.HA)

    async def parse_and_insert(self):
        await super().parse_and_insert()


class HaRegionDmsCsvParser(DmsFileParser):
    def __init__(self, filename: str):
        super().__init__(filename, ',', DefaultGffFeaturesByRegion.HA)

    async def parse_and_insert(self):
        await super().parse_and_insert()
