from DB.inserts.file_parsers.dms_parser import DmsFileParser
from utils.constants import PhenotypeMetricAssayTypes, ColumnNames, Sc2GffFeatures, Sc2PhenoMetricNames


class Hu1RbdDmsCsvParser(DmsFileParser):
    """
    https://github.com/jbloomlab/SARS-CoV-2-RBD_DMS_Omicron/blob/main/results/final_variant_scores/final_variant_scores.csv

    Reads the Wuhan-Hu-1 targets. _v1 and _v2 are library replicates of the same genotype, so
    they share this feature: _v2 comes first in the file and _v1 overwrites it.
    """
    def __init__(self, filename: str):
        super().__init__(filename, ',', Sc2GffFeatures.spike_hu1)

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

    filter_columns_values = {
        'target': {'Omicron_BA1'}
    }

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

    filter_columns_values = {
        'target': {'Omicron_BA2'}
    }

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

    required_column_name_map = {
        ColumnNames.position_aa: 'site',
        ColumnNames.ref_aa: 'wildtype',
        ColumnNames.alt_aa: 'mutant',
    }

    data_column_name_map = {
        Sc2PhenoMetricNames.kp3_spike_pre_vaccination_sera_escape: 'Pre vaccination or infection escape',
        Sc2PhenoMetricNames.kp3_spike_post_vaccination_sera_escape: 'Post vaccination or infection escape',
    }
