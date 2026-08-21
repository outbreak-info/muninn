import re
import os
from datetime import datetime
from enum import Enum
from warnings import deprecated

from utils.dates_and_times import format_iso_month, format_iso_week, format_iso_interval


class Env:
    MUNINN_DB_READONLY_USER = os.environ['MUNINN_DB_READONLY_USER']
    MUNINN_DB_READONLY_PASSWORD = os.environ['MUNINN_DB_READONLY_PASSWORD']
    MUNINN_DB_SUPERUSER_PASSWORD = os.environ['MUNINN_DB_SUPERUSER_PASSWORD']
    MUNINN_DB_SUPERUSER = os.environ['MUNINN_DB_SUPERUSER']
    MUNINN_DB_PORT = os.environ['MUNINN_DB_PORT']
    MUNINN_DB_HOST = os.environ['MUNINN_DB_HOST']
    MUNINN_DB_NAME = os.environ['MUNINN_DB_NAME']
    MUNINN_SERVER_DATA_INPUT_DIR = os.environ['MUNINN_SERVER_DATA_INPUT_DIR']


CHANGE_PATTERN = r'^([\w.-]+):([a-zA-Z])(\d+)([a-zA-Z\-+]+)'
WORDLIKE_PATTERN = re.compile(r'^\w+$')
COMMA_SEP_WORDLIKE_PATTERN = re.compile(r'^(\w+,)*\w+$')
# these dates are "simple" b/c they are a single timestamp and not null
SIMPLE_DATE_FIELDS = {'release_date', 'creation_date'}
# Unlike the simple dates, collection date is a range, and may be null
COLLECTION_DATE = 'collection_date'
GEO_LOCATION = 'geo_location'
LINEAGE = 'lineage'
DEFAULT_MAX_SPAN_DAYS = 366
DEFAULT_DAYS = 1
DEFAULT_PREVALENCE_THRESHOLD = 0.75
MIN_PREVALENCE_THRESHOLD = 0.01
# Below this, a fuzzy /v1/distinctValues `search` hit is noise rather than a typo of what the caller
# meant. Measured against real values: typos land at 0.87-0.96 ('califronia'/'California' = 0.90)
# while unrelated pairs top out around 0.56 ('cattle'/'Castor fiber' = 0.44).
MIN_FUZZY_MATCH_SCORE = 0.7
ASYNCPG_MAX_QUERY_ARGS = 32767

FILTER_SYNTAX_HELP = (
    'Filter mini-language: combine terms with ^ (AND) and | (OR); negate a single term with a prefix '
    '! (NOT is unary, so write AND-NOT as a ^ !b, not as a ! b); group with (); '
    'compare with = != > < >= <= (the ordered operators > < >= <= require a number or a YYYY-MM-DD date). '
    'String values are written bare/unquoted (e.g. host = Homo sapiens); numbers and dates are bare too. '
    'The words "and"/"or"/"not" are NOT operators and will raise a 400 error. There is no IN operator and '
    'no null test OR the alternatives instead. Which column names are accepted depends on the endpoint '
    '(see the columns listed above); enumerate the valid values for a column with GET /v1/distinctValues. '
    'Example: bases > 1000 ^ host = Homo sapiens . '
    'When placed in a URL, percent-encode the operators (space -> %20, ^ -> %5E, | -> %7C, ! -> %21, > -> %3E).'
)
NUCLEOTIDE_CHARACTERS = ['A', 'C', 'G', 'T']
# https://en.wikipedia.org/wiki/Nucleic_acid_notation
NUCLEOTIDE_CHARACTERS_AMBIGUOUS = ['A', 'C', 'G', 'T', 'M', 'R', 'W', 'S', 'Y', 'K', 'B', 'D', 'H', 'V', 'N']
CONTAINER_DATA_DIRECTORY = '/home/muninn/data'


class PhenotypeMetricAssayTypes:
    DMS = 'DMS'
    EVE = 'EVEscape'


class DefaultGffFeaturesByRegion:
    HA = 'XAJ25415.1'
    PB2 = 'XAJ25426.1'


class LineageSystemNames:
    usda_genoflu = 'usda_genoflu'
    freyja_demixed = 'freyja_demixed'
    pango = 'PANGO'


class DateBinOpt(Enum):
    month = 'month'
    week = 'week'
    day = 'day'

    def __init__(self, value):
        self._format_fn = None
        match value:
            case 'month':
                self._format_fn = format_iso_month
            case 'week':
                self._format_fn = format_iso_week
            case 'day':
                self._format_fn = format_iso_interval

    def __str__(self):
        return str(self.value)

    def format_iso_chunk(
        self,
        a: int | datetime,
        b: int | datetime
    ):
        return self._format_fn(a, b)


class NtOrAa(Enum):
    nt = 'nt'
    aa = 'aa'

    def __str__(self):
        return str(self.value)


class WastewaterGeoBin(Enum):
    """
    Geographic grouping for the averaged wastewater abundances. v0 took this as a bare string and
    raised ValueError (a 500) on anything else; as an enum a bad value is a 422 and the two legal
    values show up in the schema.
    """
    admin1_name = 'admin1_name'
    census_region = 'census_region'

    def __str__(self):
        return str(self.value)


class DistinctValueField(Enum):
    """
    Whitelist of columns whose distinct values can be enumerated via GET /v1/distinctValues.
    These are the high-value, bounded-cardinality string columns an LLM/user needs when building
    a `filter` query. The value <-> (table, column) mapping lives in DB.queries.helpers.
    """
    # samples metadata
    host = 'host'
    organism = 'organism'
    serotype = 'serotype'
    platform = 'platform'
    instrument = 'instrument'
    assay_type = 'assay_type'
    library_selection = 'library_selection'
    library_source = 'library_source'
    library_layout = 'library_layout'
    isolation_source = 'isolation_source'
    center_name = 'center_name'
    bio_project = 'bio_project'
    # geo_locations (joined onto samples)
    country_name = 'country_name'
    admin1_name = 'admin1_name'
    admin2_name = 'admin2_name'
    admin3_name = 'admin3_name'
    # genomic change dimensions
    region = 'region'
    gff_feature = 'gff_feature'
    # lineage nomenclature
    lineage_system_name = 'lineage_system_name'

    def __str__(self):
        return str(self.value)


class StandardPhenoMetricNames:
    species_sera_escape = 'species_sera_escape'
    entry_in_293t_cells = 'entry_in_293t_cells'
    stability = 'stability'
    sa26_usage_increase = 'sa26_usage_increase'
    mature_h5_site = 'mature_h5_site'
    ferret_sera_escape = 'ferret_sera_escape'
    mouse_sera_escape = 'mouse_sera_escape'
    entry_in_sa26_and_sa23_293t_cells = 'entry_in_sa26_and_sa23_293t_cells'
    entry_in_neuac = 'entry_in_neuac'
    entry_in_neugc = 'entry_in_neugc'
    neugc_usage_increase = 'neugc_usage_increase'
    mutdiffsel = 'mutdiffsel'


class PgIdentifiers(object):
    @classmethod
    def _check_id_lengths(cls):
        for attribute in cls.__dict__.keys():
            if attribute[:2] != '__':
                value = getattr(cls, attribute)
                if type(value) == str:
                    if len(value) > 63:
                        raise ValueError(f'Postgres identifier should have length <= 63 bytes: {value}')


class TableNames(PgIdentifiers):
    samples = 'samples'
    alleles = 'alleles'
    amino_acids = 'amino_acids'
    cns_samples_by_allele = 'cns_samples_by_allele'
    cns_alleles_by_sample = 'cns_alleles_by_sample'
    geo_locations = 'geo_locations'
    phenotype_metrics = 'phenotype_metrics'
    phenotype_metric_values = 'phenotype_metric_values'
    lineage_systems = 'lineage_systems'
    lineages = 'lineages'
    samples_lineages = 'samples_lineages'
    papers = 'papers'
    effects = 'effects'
    annotations = 'annotations'
    annotations_papers = 'annotations_papers'
    annotations_amino_acids = 'annotations_amino_acids'
    lineages_immediate_children = 'lineages_immediate_children'
    lineages_deep_children = 'lineages_deep_children'  # actually a view.
    cns_samples_by_amino_acid = 'cns_samples_by_amino_acid'
    cns_amino_acids_by_sample = 'cns_amino_acids_by_sample'
    ih_samples_by_allele = 'ih_samples_by_allele'
    ih_samples_by_amino_acid = 'ih_samples_by_amino_acid'

    # Caches
    cache_cns_pmv_sums = 'cache_cns_pmv_sums'


class ColumnNames(PgIdentifiers):
    # ids
    sample_id = 'sample_id'
    allele_id = 'allele_id'
    amino_acid_id = 'amino_acid_id'
    phenotype_metric_id = 'phenotype_metric_id'
    lineage_system_id = 'lineage_system_id'
    lineage_id = 'lineage_id'
    effect_id = 'effect_id'
    paper_id = 'paper_id'
    annotation_id = 'annotation_id'

    # samples
    accession = 'accession'
    bio_project = 'bio_project'
    bio_sample = 'bio_sample'
    bio_sample_accession = 'bio_sample_accession'
    bio_sample_model = 'bio_sample_model'
    center_name = 'center_name'
    experiment = 'experiment'
    host = 'host'
    instrument = 'instrument'
    platform = 'platform'
    isolate = 'isolate'
    library_name = 'library_name'
    library_layout = 'library_layout'
    library_selection = 'library_selection'
    library_source = 'library_source'
    organism = 'organism'
    is_retracted = 'is_retracted'
    retraction_detected_date = 'retraction_detected_date'
    isolation_source = 'isolation_source'
    collection_start_date = 'collection_start_date'
    collection_end_date = 'collection_end_date'
    release_date = 'release_date'
    creation_date = 'creation_date'
    version = 'version'
    sample_name = 'sample_name'
    sra_study = 'sra_study'
    serotype = 'serotype'
    geo_location_id = 'geo_location_id'
    assay_type = 'assay_type'
    avg_spot_length = 'avg_spot_length'
    bases = 'bases'
    ww_viral_load = 'ww_viral_load'
    ww_catchment_population = 'ww_catchment_population'
    ww_site_id = 'ww_site_id'
    ww_collected_by = 'ww_collected_by'
    census_region = 'census_region'

    # alleles
    position_nt = 'position_nt'
    ref_nt = 'ref_nt'
    alt_nt = 'alt_nt'
    region = 'region'

    # amino subs
    gff_feature = 'gff_feature'
    ref_codon = 'ref_codon'
    alt_codon = 'alt_codon'
    ref_aa = 'ref_aa'
    alt_aa = 'alt_aa'
    position_aa = 'position_aa'

    # variants
    ref_dp = 'ref_dp'
    alt_dp = 'alt_dp'
    alt_freq = 'alt_freq'
    ref_rv = 'ref_rv'
    alt_rv = 'alt_rv'
    ref_qual = 'ref_qual'
    alt_qual = 'alt_qual'
    total_dp = 'total_dp'
    pval = 'pval'
    pass_qc = 'pass_qc'

    # variants / mutations bitmap
    samples_present = 'samples_present'
    alleles_present = 'alleles_present'
    amino_acids_present = 'amino_acids_present'

    # geo locations
    country_name = 'country_name'
    admin1_name = 'admin1_name'
    admin2_name = 'admin2_name'
    admin3_name = 'admin3_name'

    # phenotype metrics
    phenotype_metric_name = 'phenotype_metric_name'
    phenotype_metric_assay_type = 'phenotype_metric_assay_type'

    # papers
    authors = 'authors'
    publication_year = 'publication_year'
    title = 'title'

    # effects
    detail = 'detail'

    # Lineages
    lineage_system_name = 'lineage_system_name'
    lineage_name = 'lineage_name'
    is_consensus_call = 'is_consensus_call'
    abundance = 'abundance'

    # lineage hierarchy
    parent_id = 'parent_id'
    child_id = 'child_id'


class ConstraintNames(PgIdentifiers):
    # primary keys
    pk_samples = f'pk_{TableNames.samples}'
    pk_alleles = f'pk_{TableNames.alleles}'
    pk_amino_acids = f'pk_{TableNames.amino_acids}'
    pk_cns_samples_by_allele = f'pk_{TableNames.cns_samples_by_allele}'
    pk_geo_locations = f'pk_{TableNames.geo_locations}'
    pk_phenotype_metrics = f'pk_{TableNames.phenotype_metrics}'
    pk_phenotype_metric_values = f'pk_{TableNames.phenotype_metric_values}'
    pk_lineage_systems = f'pk_{TableNames.lineage_systems}'
    pk_lineages = f'pk_{TableNames.lineages}'
    pk_samples_lineages = f'pk_{TableNames.samples_lineages}'
    pk_papers = f'pk_{TableNames.papers}'
    pk_effects = f'pk_{TableNames.effects}'
    pk_annotations = f'pk_{TableNames.annotations}'
    pk_annotations_papers = f'pk_{TableNames.annotations_papers}'
    pk_annotations_amino_acids = f'pk_{TableNames.annotations_amino_acids}'
    pk_lineages_immediate_children = f'pk_{TableNames.lineages_immediate_children}'
    pk_cns_samples_by_amino_acid = f'pk_{TableNames.cns_samples_by_amino_acid}'

    pk_cns_alleles_by_sample = f'pk_{TableNames.cns_alleles_by_sample}'
    pk_cns_amino_acids_by_sample = f'pk_{TableNames.cns_amino_acids_by_sample}'
    pk_ih_samples_by_allele = f'pk_{TableNames.ih_samples_by_allele}'
    pk_ih_samples_by_amino_acid = f'pk_{TableNames.ih_samples_by_amino_acid}'

    # samples
    uq_samples_accession = 'uq_samples_accession'
    fk_samples_geo_location_id_geo_locations = 'fk_samples_geo_location_id_geo_locations'
    ck_samples_retraction_values_existence_in_harmony = 'ck_samples_retraction_values_existence_in_harmony'
    ck_samples_collection_start_and_end_both_absent_or_both_present = 'ck_samples_collection_start_and_end_both_absent_or_both_present'
    ck_samples_collection_start_not_after_collection_end = 'ck_samples_collection_start_not_after_collection_end'

    # alleles
    ck_alleles_alt_nt_not_empty = 'ck_alleles_alt_nt_not_empty'
    ck_alleles_ref_nt_not_empty = 'ck_alleles_ref_nt_not_empty'
    uq_alleles_nt_values = 'uq_alleles_nt_values'

    # amino acids
    ck_amino_acids_gff_feature_not_empty = 'ck_amino_acids_gff_feature_not_empty'
    ck_amino_acids_ref_aa_not_empty = 'ck_amino_acids_ref_aa_not_empty'
    ck_amino_acids_alt_aa_not_empty = 'ck_amino_acids_alt_aa_not_empty'
    ck_amino_acids_alt_codon_not_empty = 'ck_amino_acids_alt_codon_not_empty'
    ck_amino_acids_ref_codon_not_empty = 'ck_amino_acids_ref_codon_not_empty'
    uq_amino_acids_gff_feature_position_alt_aa_alt_codon = 'uq_amino_acids_gff_feature_position_alt_aa_alt_codon'

    # intra host variants
    fk_intra_host_variants_allele_id_alleles = 'fk_intra_host_variants_allele_id_alleles'
    fk_intra_host_variants_sample_id_samples = 'fk_intra_host_variants_sample_id_samples'

    # intrahost samples by allele
    fk_ih_samples_by_allele_allele_id_alleles = 'fk_ih_samples_by_allele_allele_id_alleles'

    # intrahost samples by amino acid
    fk_ih_samples_by_amino_acid_amino_acid_id_amino_acids = 'fk_ih_samples_by_amino_acid_amino_acid_id_amino_acids'

    # consensus samples by allele
    fk_cns_samples_by_allele_allele_id_alleles = 'fk_cns_samples_by_allele_allele_id_alleles'

    # consensus samples by amino acid
    fk_cns_samples_by_amino_acid_amino_acid_id_amino_acids = 'fk_cns_samples_by_amino_acid_amino_acid_id_amino_acids'

    # consensus amino acids by sample
    fk_cns_amino_acids_by_sample_sample_id_samples = 'fk_cns_amino_acids_by_sample_sample_id_samples'

    # phenotype metrics tables
    uq_phenotype_metrics_name = 'uq_phenotype_metrics_name'
    ck_phenotype_metrics_name_not_empty = 'ck_phenotype_metrics_name_not_empty'
    ck_phenotype_metrics_assay_type_not_empty = 'ck_phenotype_metrics_assay_type_not_empty'
    uq_phenotype_metric_values_metric_and_amino_acid = f'uq_{TableNames.phenotype_metric_values}_metric_and_amino_acid'
    fk_phenotype_metric_values_amino_acid_id_amino_acids = 'fk_phenotype_metric_values_amino_acid_id_amino_acids'
    fk_phenotype_metric_values_phenotype_metric_id_pheno_metrics = 'fk_phenotype_metric_values_phenotype_metric_id_pheno_metrics'

    # geo locations
    uq_geo_locations_division_names = 'uq_geo_locations_division_names'

    # lineages tables
    uq_lineage_systems_name = 'uq_lineage_systems_name'
    uq_lineages_name_uq_within_system = 'uq_lineages_name_uq_within_system'
    uq_samples_lineages_sample_id_lineage_id_is_consensus_call = 'uq_samples_lineages_sample_id_lineage_id_is_consensus_call'
    ck_samples_lineages_has_abundance_xor_consensus = f'ck_{TableNames.samples_lineages}_has_abundance_xor_is_consensus'
    uq_lineages_immediate_children_parent_child = f'uq_{TableNames.lineages_immediate_children}_parent_child'
    ck_lineages_immediate_children_no_self_parenthood = f'ck_{TableNames.lineages_immediate_children}_no_self_parenthood'

    # annotations tables
    uq_papers_authors_title_year = 'uq_papers_authors_title_year'
    uq_effects_detail = 'uq_effects_detail'
    uq_annotations_papers_annotation_paper_pair = 'uq_annotations_papers_annotation_paper_pair'
    uq_annotations_amino_acids_pair = 'uq_annotations_amino_acids_pair'
    fk_annotations_amino_acids_amino_acid_id_amino_acids = 'fk_annotations_amino_acids_amino_acid_id_amino_acids'


class IndexNames(PgIdentifiers):
    # samples lineages
    ix_samples_lineages_lineage_id = 'ix_samples_lineages_lineage_id'


class MiscDbNames(PgIdentifiers):
    check_cyclic_lineage = 'check_cyclic_lineage'
    check_cyclic_lineage_trigger = 'check_cyclic_lineage_trigger'
    check_cross_system_lineage = 'check_cross_system_lineage'
    check_cross_system_lineage_trigger = 'check_cross_system_lineage_trigger'


# Keep this after all pg identifier definitions
for cls in PgIdentifiers.__subclasses__():
    cls._check_id_lengths()

# Problematic redacted SRAs
EXCLUDED_SRAS = {
    'SRR28752471', 'SRR28752477', 'SRR28752528', 'SRR28752549', 'SRR29182424', 'SRR29182425',
    'SRR29182426', 'SRR29182427', 'SRR29182428', 'SRR29182429', 'SRR29182430', 'SRR29182431',
    'SRR29182432', 'SRR29182433', 'SRR29182434', 'SRR29182435', 'SRR29182436', 'SRR29182437',
    'SRR29182438', 'SRR29182439', 'SRR29182440', 'SRR29182441', 'SRR29182442', 'SRR29182443',
    'SRR29182444', 'SRR29182445', 'SRR29182446', 'SRR29182447', 'SRR29182448', 'SRR29182449',
    'SRR29182450', 'SRR29182451', 'SRR29182452', 'SRR29182453', 'SRR29182454', 'SRR29182455',
    'SRR29182456', 'SRR29182457', 'SRR29182458', 'SRR29182459', 'SRR29182460', 'SRR29182461',
    'SRR29182462', 'SRR29182463', 'SRR29182464', 'SRR29182465', 'SRR29182466', 'SRR29182467',
    'SRR29182468', 'SRR29182469', 'SRR29182470', 'SRR29182471', 'SRR29182472', 'SRR29182473',
    'SRR29182474', 'SRR29182475', 'SRR29182476', 'SRR29182477', 'SRR29182478', 'SRR29182479',
    'SRR29182480', 'SRR29182481', 'SRR29182482', 'SRR29182483', 'SRR29182484', 'SRR29182485',
}

CODONS_AMINO_ACIDS = [
    ('TTT', 'F'), ('TTC', 'F'), ('TTA', 'L'), ('TTG', 'L'),
    ('CTT', 'L'), ('CTC', 'L'), ('CTA', 'L'), ('CTG', 'L'),
    ('ATT', 'I'), ('ATC', 'I'), ('ATA', 'I'), ('ATG', 'M'),
    ('GTT', 'V'), ('GTC', 'V'), ('GTA', 'V'), ('GTG', 'V'),
    ('TCT', 'S'), ('TCC', 'S'), ('TCA', 'S'), ('TCG', 'S'),
    ('CCT', 'P'), ('CCC', 'P'), ('CCA', 'P'), ('CCG', 'P'),
    ('ACT', 'T'), ('ACC', 'T'), ('ACA', 'T'), ('ACG', 'T'),
    ('GCT', 'A'), ('GCC', 'A'), ('GCA', 'A'), ('GCG', 'A'),
    ('TAT', 'Y'), ('TAC', 'Y'), ('TAA', '*'), ('TAG', '*'),
    ('CAT', 'H'), ('CAC', 'H'), ('CAA', 'Q'), ('CAG', 'Q'),
    ('AAT', 'N'), ('AAC', 'N'), ('AAA', 'K'), ('AAG', 'K'),
    ('GAT', 'D'), ('GAC', 'D'), ('GAA', 'E'), ('GAG', 'E'),
    ('TGT', 'C'), ('TGC', 'C'), ('TGA', '*'), ('TGG', 'W'),
    ('CGT', 'R'), ('CGC', 'R'), ('CGA', 'R'), ('CGG', 'R'),
    ('AGT', 'S'), ('AGC', 'S'), ('AGA', 'R'), ('AGG', 'R'),
    ('GGT', 'G'), ('GGC', 'G'), ('GGA', 'G'), ('GGG', 'G')
]
