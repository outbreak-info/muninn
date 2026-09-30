from csv import DictReader
from typing import Set, Dict

from DB.inserts.file_parsers.file_parser import FileParser
from DB.inserts.phenotype_measurement_results import insert_pheno_measurement_result
from DB.inserts.phenotype_metrics import find_or_insert_metric
from DB.models import AminoAcid, PhenotypeMetric, PhenotypeMetricValues
from DB.inserts.amino_acids import find_equivalent_amino_acids
from utils.constants import PhenotypeMetricAssayTypes, ColumnNames, Sc2GffFeatures, Sc2PhenoMetricNames
from utils.csv_helpers import get_value, clean_up_gff_feature
from utils.errors import NotFoundError


class DmsFileParser(FileParser):

    def __init__(
        self,
        filename: str,
        delimiter: str,
        gff_feature: str,
        assay_type: str = PhenotypeMetricAssayTypes.DMS
    ):
        self.filename = filename
        self.delimiter = delimiter
        self.gff_feature = clean_up_gff_feature(gff_feature)
        self.assay_type = assay_type

    async def parse_and_insert(self):
        debug_info = {
            'skipped_aas_data_missing': 0,
            'skipped_other_target': 0,
            'skipped_aas_not_found': 0,
            'value_parsing_errors': 0,
            'count_existing_updated': 0,  # only counts if the value changed
            'count_new_records_inserted': 0
        }
        # format: metric_name -> id
        cache_metric_ids = dict()
        # format: (position_aa, ref_aa, alt_aa, gff_feature) -> {amino acid ids} set
        cache_amino_sub_ids = dict()
        # format: (position_aa, ref_aa, alt_aa, gff_feature)
        cache_amino_subs_not_found = set()
        with open(self.filename, 'r') as f:
            reader = DictReader(f, delimiter=self.delimiter)
            self._verify_header(reader)
            present_data_cols = self._get_present_data_columns(reader)

            gff_feature_col = self.required_column_name_map.get(ColumnNames.gff_feature)

            for row in reader:
                if self.target_column is not None and not row.get(self.target_column) in self.targets:
                    debug_info['skipped_other_target'] += 1
                    continue
                try:
                    position_aa = get_value(
                        row,
                        self.required_column_name_map[ColumnNames.position_aa],
                        transform=int
                    )
                    ref_aa = get_value(row, self.required_column_name_map[ColumnNames.ref_aa])
                    alt_aa = get_value(row, self.required_column_name_map[ColumnNames.alt_aa])
                    gff_feature = get_value(row, self.required_column_name_map[ColumnNames.gff_feature])
                except ValueError:
                    debug_info['skipped_aas_data_missing'] += 1
                    continue

                if (position_aa, ref_aa, alt_aa, gff_feature) in cache_amino_subs_not_found:
                    debug_info['skipped_aas_not_found'] += 1
                    continue
                try:
                    amino_acid_ids = cache_amino_sub_ids[(position_aa, ref_aa, alt_aa, gff_feature)]
                except KeyError:
                    try:
                        amino_acid_ids = await find_equivalent_amino_acids(
                            AminoAcid(
                                gff_feature=gff_feature,
                                position_aa=position_aa,
                                alt_aa=alt_aa,
                                ref_aa=ref_aa
                            )
                        )
                        cache_amino_sub_ids[(position_aa, ref_aa, alt_aa, gff_feature)] = amino_acid_ids
                    except NotFoundError:
                        # if the aas doesn't already exist, skip the record.
                        # we don't want to create orphaned aas entries just for the dms data
                        debug_info['skipped_aas_not_found'] += 1
                        cache_amino_subs_not_found.add((position_aa, ref_aa, alt_aa, gff_feature))
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
                                phenotype_metric_name=canonical_name,
                                phenotype_metric_assay_type=self.assay_type
                            )
                        )
                        cache_metric_ids[canonical_name] = metric_id

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
        cols = set(cls.required_column_name_map.values())
        if cls.target_column is not None:
            cols.add(cls.target_column)
        return cols

    # these can be overridden as required in subclasses
    target_column = None
    targets = frozenset()
    required_column_name_map = {
        ColumnNames.position_aa: 'position',
        ColumnNames.ref_aa: 'wildtype',
        ColumnNames.alt_aa: 'mutant',
        ColumnNames.gff_feature: 'GFF_FEATURE',
    }

class Sc2DmsTsvParser(DmsFileParser):
    def __init__(self, filename: str):
        super().__init__(filename, '\t', '')

    async def parse_and_insert(self):
        await super().parse_and_insert()

    data_column_name_map = {
        'delta_bind': 'delta_bind',
        'delta_expr': 'delta_expr',
    }


class Hu1RbdDmsCsvParser(DmsFileParser):
    """
    https://github.com/jbloomlab/SARS-CoV-2-RBD_DMS_Omicron/blob/main/results/final_variant_scores/final_variant_scores.csv

    Reads the Wuhan-Hu-1 targets. _v1 and _v2 are library replicates of the same genotype, so
    they share this feature: _v2 comes first in the file and _v1 overwrites it.
    """
    def __init__(self, filename: str):
        super().__init__(filename, ',', Sc2GffFeatures.spike_hu1)

    async def parse_and_insert(self):
        await super().parse_and_insert()

    target_column = 'target'
    targets = {'Wuhan-Hu-1_v1', 'Wuhan-Hu-1_v2'}
    required_column_name_map = {
        ColumnNames.position_aa: 'position',
        ColumnNames.ref_aa: 'wildtype',
        ColumnNames.alt_aa: 'mutant',
    }
    data_column_name_map = {
        Sc2PhenoMetricNames.delta_bind: 'delta_bind',
        Sc2PhenoMetricNames.delta_expr: 'delta_expr',
    }


class Ba1RbdDmsCsvParser(DmsFileParser):
    """
    https://github.com/jbloomlab/SARS-CoV-2-RBD_DMS_Omicron/blob/main/results/final_variant_scores/final_variant_scores.csv

    Reads the Omicron_BA1 target.
    """
    def __init__(self, filename: str):
        super().__init__(filename, ',', Sc2GffFeatures.rbd_ba1)

    async def parse_and_insert(self):
        await super().parse_and_insert()

    target_column = 'target'
    targets = {'Omicron_BA1'}
    required_column_name_map = {
        ColumnNames.position_aa: 'position',
        ColumnNames.ref_aa: 'wildtype',
        ColumnNames.alt_aa: 'mutant',
    }
    data_column_name_map = {
        Sc2PhenoMetricNames.delta_bind: 'delta_bind',
        Sc2PhenoMetricNames.delta_expr: 'delta_expr',
    }


class Ba2RbdDmsCsvParser(DmsFileParser):
    """
    https://github.com/jbloomlab/SARS-CoV-2-RBD_DMS_Omicron/blob/main/results/final_variant_scores/final_variant_scores.csv

    Reads the Omicron_BA2 target.
    """
    def __init__(self, filename: str):
        super().__init__(filename, ',', Sc2GffFeatures.rbd_ba2)

    async def parse_and_insert(self):
        await super().parse_and_insert()

    target_column = 'target'
    targets = {'Omicron_BA2'}
    required_column_name_map = {
        ColumnNames.position_aa: 'position',
        ColumnNames.ref_aa: 'wildtype',
        ColumnNames.alt_aa: 'mutant',
    }
    data_column_name_map = {
        Sc2PhenoMetricNames.delta_bind: 'delta_bind',
        Sc2PhenoMetricNames.delta_expr: 'delta_expr',
    }


class Hu1SpikeEveScapeCsvParser(DmsFileParser):
    """
    https://github.com/OATML-Markslab/EVEscape/blob/main/results/summaries_with_scores/full_spike_evescape.csv
    """
    def __init__(self, filename: str):
        super().__init__(filename, ',', Sc2GffFeatures.spike_hu1, PhenotypeMetricAssayTypes.EVE)

    async def parse_and_insert(self):
        await super().parse_and_insert()

    required_column_name_map = {
        ColumnNames.position_aa: 'i',
        ColumnNames.ref_aa: 'wt',
        ColumnNames.alt_aa: 'mut',
    }
    data_column_name_map = {
        Sc2PhenoMetricNames.evescape: 'evescape',
    }


class Ba2SpikeDmsCsvParser(DmsFileParser):
    """
    https://github.com/dms-vep/SARS-CoV-2_Omicron_BA.2_spike_ACE2_binding/blob/main/results/summaries/summary.csv
    """
    def __init__(self, filename: str):
        super().__init__(filename, ',', Sc2GffFeatures.spike_ba2)

    async def parse_and_insert(self):
        await super().parse_and_insert()

    required_column_name_map = {
        ColumnNames.position_aa: 'site',
        ColumnNames.ref_aa: 'wildtype',
        ColumnNames.alt_aa: 'mutant',
    }
    data_column_name_map = {
        Sc2PhenoMetricNames.ba2_spike_mediated_entry: 'spike mediated entry',
        Sc2PhenoMetricNames.ba2_spike_ace2_binding: 'ACE2 binding',
    }


class Xbb15RbdDmsCsvParser(DmsFileParser):
    """
    https://github.com/dms-vep/SARS-CoV-2_XBB.1.5_RBD_DMS/blob/main/results/summaries/summary.csv
    """
    def __init__(self, filename: str):
        super().__init__(filename, ',', Sc2GffFeatures.rbd_xbb15)

    async def parse_and_insert(self):
        await super().parse_and_insert()

    required_column_name_map = {
        ColumnNames.position_aa: 'site',
        ColumnNames.ref_aa: 'wildtype',
        ColumnNames.alt_aa: 'mutant',
    }
    data_column_name_map = {
        Sc2PhenoMetricNames.xbb15_rbd_human_sera_escape: 'human sera escape',
        Sc2PhenoMetricNames.xbb15_rbd_spike_mediated_entry: 'spike mediated entry',
        Sc2PhenoMetricNames.xbb15_rbd_monomeric_ace2_binding: 'monomeric ACE2 binding',
        Sc2PhenoMetricNames.xbb15_rbd_dimeric_ace2_binding: 'dimeric ACE2 binding',
    }


class Xbb15SpikeDmsCsvParser(DmsFileParser):
    """
    https://github.com/dms-vep/SARS-CoV-2_XBB.1.5_spike_DMS/blob/main/results/summaries/summary.csv
    """
    def __init__(self, filename: str):
        super().__init__(filename, ',', Sc2GffFeatures.spike_xbb15)

    async def parse_and_insert(self):
        await super().parse_and_insert()

    required_column_name_map = {
        ColumnNames.position_aa: 'site',
        ColumnNames.ref_aa: 'wildtype',
        ColumnNames.alt_aa: 'mutant',
    }
    data_column_name_map = {
        Sc2PhenoMetricNames.xbb15_spike_human_sera_escape: 'human sera escape',
        Sc2PhenoMetricNames.xbb15_spike_mediated_entry: 'spike mediated entry',
        Sc2PhenoMetricNames.xbb15_spike_ace2_binding: 'ACE2 binding',
    }


class Kp3SpikeAntibodyEscapeCsvParser(DmsFileParser):
    """
    https://github.com/dms-vep/SARS-CoV-2_KP.3.1.1_spike_DMS/blob/main/results/summaries/antibody_escape.csv
    """
    def __init__(self, filename: str):
        super().__init__(filename, ',', Sc2GffFeatures.spike_kp3)

    async def parse_and_insert(self):
        await super().parse_and_insert()

    required_column_name_map = {
        ColumnNames.position_aa: 'site',
        ColumnNames.ref_aa: 'wildtype',
        ColumnNames.alt_aa: 'mutant',
    }
    data_column_name_map = {
        Sc2PhenoMetricNames.kp3_spike_bd55_1205_escape: 'BD55-1205',
        Sc2PhenoMetricNames.kp3_spike_sa55_escape: 'SA55',
        Sc2PhenoMetricNames.kp3_spike_vyd222_escape: 'VYD222',
        Sc2PhenoMetricNames.kp3_spike_mean_antibody_escape: 'mean antibodies',
        Sc2PhenoMetricNames.kp3_spike_mediated_entry: 'spike mediated entry',
        Sc2PhenoMetricNames.kp3_spike_ace2_binding: 'ACE2 binding',
    }


class Kp3SpikeSeraEscapeCsvParser(DmsFileParser):
    """
    https://github.com/dms-vep/SARS-CoV-2_KP.3.1.1_spike_DMS/blob/main/results/summaries/sera_group_averages.csv

    'spike mediated entry' and 'ACE2 binding' are identical to antibody_escape.csv on all shared
    keys, so they are ingested from that file only.
    """
    def __init__(self, filename: str):
        super().__init__(filename, ',', Sc2GffFeatures.spike_kp3)

    async def parse_and_insert(self):
        await super().parse_and_insert()

    required_column_name_map = {
        ColumnNames.position_aa: 'site',
        ColumnNames.ref_aa: 'wildtype',
        ColumnNames.alt_aa: 'mutant',
    }
    data_column_name_map = {
        Sc2PhenoMetricNames.kp3_spike_pre_vaccination_sera_escape: 'Pre vaccination or infection escape',
        Sc2PhenoMetricNames.kp3_spike_post_vaccination_sera_escape: 'Post vaccination or infection escape',
    }
