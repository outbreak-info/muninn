from csv import DictReader
from typing import Set, Dict

from DB.inserts.amino_acids import find_equivalent_amino_acids
from DB.inserts.file_parsers.file_parser import FileParser
from DB.inserts.phenotype_measurement_results import upsert_pheno_measurement_result
from DB.inserts.phenotype_metrics import find_or_insert_metric
from utils.constants import PhenotypeMetricAssayTypes, ColumnNames, \
    StandardPhenoMetricNames
from utils.csv_helpers import get_value
from utils.errors import NotFoundError


class DmsFileParser(FileParser):

    def __init__(
        self,
        filename: str,
        delimiter: str = '\t',
        gff_feature: str = None,
        assay_type: str = PhenotypeMetricAssayTypes.DMS,
        extra_args: list[str] = None
    ):
        """
        :param filename:
        :param delimiter:
        :param gff_feature: str. Which gff feature to match in amino acids. If both this and extras provide a value,
        this will take priority.
        :param assay_type:
        :param extra_args: list[str] may provide gff feature in format `gff_feature=value`.
        Overridden by gff_feature param.
        """
        self.filename = filename
        self.delimiter = delimiter
        self.assay_type = assay_type

        if gff_feature is None:
            self.gff_feature = self._get_gff_from_extras(extra_args)
        else:
            self.gff_feature = gff_feature

    async def parse_and_insert(self):
        debug_info = {
            'skipped_aa_data_missing': 0,
            'skipped_aa_not_found': 0,
            'value_parsing_errors': 0,
            'count_existing_updated': 0,  # only counts if the value changed
            'count_new_records_inserted': 0,
            'count_skipped_by_filter': 0
        }
        # format: metric_name -> id
        cache_metric_ids = dict()
        # format: (position_aa, ref_aa, alt_aa) -> {amino acid ids} set
        cache_amino_sub_ids = dict()
        # format: (position_aa, ref_aa, alt_aa)
        cache_amino_subs_not_found = set()
        with open(self.filename, 'r') as f:
            reader = DictReader(f, delimiter=self.delimiter)
            self._verify_header(reader)
            present_data_cols = self._get_present_data_columns(reader)

            for row in reader:
                if self.filter_columns_values is not None:
                    for colname, allowed_values in self.filter_columns_values.items():
                        if not row[colname] in allowed_values:
                            debug_info['count_skipped_by_filter'] += 1
                            continue
                try:
                    position_aa = get_value(
                        row,
                        self.required_column_name_map[ColumnNames.position_aa],
                        transform=int
                    )
                    ref_aa = get_value(row, self.required_column_name_map[ColumnNames.ref_aa])
                    alt_aa = get_value(row, self.required_column_name_map[ColumnNames.alt_aa])
                except ValueError:
                    debug_info['skipped_aa_data_missing'] += 1
                    continue

                if (position_aa, ref_aa, alt_aa) in cache_amino_subs_not_found:
                    debug_info['skipped_aa_not_found'] += 1
                    continue
                try:
                    amino_acid_ids = cache_amino_sub_ids[(position_aa, ref_aa, alt_aa)]
                except KeyError:
                    try:
                        amino_acid_ids = await find_equivalent_amino_acids(
                            gff_feature=self.gff_feature,
                            position_aa=position_aa,
                            alt_aa=alt_aa,
                            ref_aa=ref_aa
                        )
                        cache_amino_sub_ids[(position_aa, ref_aa, alt_aa)] = amino_acid_ids
                    except NotFoundError:
                        # if the aas doesn't already exist, skip the record.
                        # we don't want to create orphaned aas entries just for the dms data
                        debug_info['skipped_aa_not_found'] += 1
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
                            phenotype_metric_name=canonical_name,
                            phenotype_metric_assay_type=self.assay_type
                        )
                        cache_metric_ids[canonical_name] = metric_id

                    for aa_id in amino_acid_ids:
                        updated = await upsert_pheno_measurement_result(
                            amino_acid_id=aa_id,
                            phenotype_metric_id=metric_id,
                            value=v
                        )
                        if updated:
                            debug_info['count_existing_updated'] += 1
                        else:
                            debug_info['count_new_records_inserted'] += 1

        debug_info['count_aas_not_found'] = len(cache_amino_subs_not_found)
        print(debug_info)

    @classmethod
    def _get_present_data_columns(cls, reader: DictReader) -> Dict[str, str]:
        actual_cols = set(reader.fieldnames)

        data_cols_present = {k: v for k, v in cls.data_column_name_map.items() if v in actual_cols}
        if len(data_cols_present) == 0:
            raise ValueError(
                f'No DMS data columns found, so no values can be extracted from this file. '
                f'Available data columns: {set(cls.data_column_name_map.values())}'
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
        required = set(cls.required_column_name_map.values())
        if cls.filter_columns_values is not None:
            required = required.union(cls.filter_columns_values.keys())
        return required

    @staticmethod
    def _get_gff_from_extras(extras: list[str]):
        for arg in extras:
            name, value = arg.split('=')
            if name == ColumnNames.gff_feature:
                return value
        raise ValueError(f'gff feature not found in extras: {extras}')

    # these can be overridden as required in subclasses
    required_column_name_map = {
        ColumnNames.position_aa: 'sequential_site',
        ColumnNames.ref_aa: 'wildtype',
        ColumnNames.alt_aa: 'mutant',
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
    # Only ingest rows with specified values in specified columns. If None, no filtering is done.
    # format: {col_name: {value1, value2}, ...}
    filter_columns_values: dict[str, set[str]] = None


class HaRegionDmsTsvParser(DmsFileParser):
    """
    https://raw.githubusercontent.com/dms-vep/Flu_H5_American-Wigeon_South-Carolina_2021-H5N1_DMS/refs/heads/main/results/summaries/all_sera_escape.csv
    converted to tsv
    todo: Not sure why we're using a version converted to tsv
    """
    def __init__(self, filename: str, extra_args: list[str]):
        super().__init__(filename, '\t', extra_args=extra_args)

    async def parse_and_insert(self):
        await super().parse_and_insert()


class HaRegionDmsCsvParser(DmsFileParser):
    """
    https://raw.githubusercontent.com/dms-vep/Flu_H5_American-Wigeon_South-Carolina_2021-H5N1_DMS/refs/heads/main/results/summaries/all_sera_escape.csv
    """
    def __init__(self, filename: str, extra_args: list[str]):
        super().__init__(filename, ',', extra_args=extra_args)

    async def parse_and_insert(self):
        await super().parse_and_insert()

# todo: fix name in runinserts, fix metric name
class HaRegionDmsCsvParserNewData(DmsFileParser):
    def __init__(self, filename: str, extra_args: list[str]):
        super().__init__(filename, ',', extra_args=extra_args)

    async def parse_and_insert(self):
        await super().parse_and_insert()

    data_column_name_map = {
        'sa26_usage_increase_new': 'SA26 usage increase',
        StandardPhenoMetricNames.entry_in_sa26_and_sa23_293t_cells: 'entry in SA26 and SA23 293T cells',
    }


class HaRegionDmsCsvParserNeuAcVsNeuGc(DmsFileParser):
    """
    https://raw.githubusercontent.com/dms-vep/Flu_H5_American-Wigeon_South-Carolina_2021-H5N1_DMS_NeuGc/refs/heads/master/results/summaries/entry_in_NeuAc_vs_NeuGc_cells.csv
    todo: double-check that this is the file
    """
    def __init__(self, filename: str, extra_args: list[str]):
        super().__init__(filename, ',', extra_args=extra_args)

    async def parse_and_insert(self):
        await super().parse_and_insert()

    data_column_name_map = {
        StandardPhenoMetricNames.entry_in_neuac: 'entry in 293 cells',
        StandardPhenoMetricNames.entry_in_neugc: 'entry in CMAH cells',
        StandardPhenoMetricNames.neugc_usage_increase: 'NeuGc usage increase',
    }


class Pb2RegionDmsCsvParser(DmsFileParser):
    """
    https://doi.org/10.7554/eLife.45079
    XAJ25426.1_PB2|CY018884.1|A/green-winged_teal/Ohio/175/1986(H2N1)
    todo: find dms results file
    """

    def __init__(self, filename: str, extra_args: list[str]):
        super().__init__(filename, ',', extra_args=extra_args)

    async def parse_and_insert(self):
        await super().parse_and_insert()

    required_column_name_map = {
        ColumnNames.position_aa: 'site',
        ColumnNames.ref_aa: 'wildtype',
        ColumnNames.alt_aa: 'mutation',
    }

    data_column_name_map = {
        StandardPhenoMetricNames.mutdiffsel: 'mutdiffsel'
    }
