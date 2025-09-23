import re
import os
from datetime import datetime
from enum import Enum

from utils.dates_and_times import format_iso_month, format_iso_week, format_iso_interval


class Env:
    MUNINN_DB_READONLY_USER = os.environ['MUNINN_DB_READONLY_USER']
    MUNINN_DB_READONLY_PASSWORD = os.environ['MUNINN_DB_READONLY_PASSWORD']
    MUNINN_DB_SUPERUSER_PASSWORD = os.environ['MUNINN_DB_SUPERUSER_PASSWORD']
    MUNINN_DB_SUPERUSER = os.environ['MUNINN_DB_SUPERUSER']
    MUNINN_DB_PORT = os.environ['MUNINN_DB_PORT']
    MUNINN_DB_HOST = os.environ['MUNINN_DB_HOST']
    MUNINN_DB_NAME = os.environ['MUNINN_DB_NAME']


CHANGE_PATTERN = r'^([\w-]+):([a-zA-Z])(\d+)([a-zA-Z\-+]+)'
WORDLIKE_PATTERN = re.compile(r'\w+')
COMMA_SEP_WORDLIKE_PATTERN = re.compile(r'(\w+,)*\w+')
# these dates are "simple" b/c they are a single timestamp and not null
SIMPLE_DATE_FIELDS = {'release_date', 'creation_date'}
# Unlike the simple dates, collection date is a range, and may be null
COLLECTION_DATE = 'collection_date'
GEO_LOCATION = 'geo_location'
LINEAGE = 'lineage'
DEFAULT_MAX_SPAN_DAYS = 366
DEFAULT_DAYS = 5
DEFAULT_PREVALENCE_THRESHOLD = 0.75
ASYNCPG_MAX_QUERY_ARGS = 32767
NUCLEOTIDE_CHARACTERS = ['A', 'C', 'G', 'T']
# https://en.wikipedia.org/wiki/Nucleic_acid_notation
NUCLEOTIDE_CHARACTERS_AMBIGUOUS = ['A', 'C', 'G', 'T', 'M', 'R', 'W', 'S', 'Y', 'K', 'B', 'D', 'H', 'V', 'N']


class PhenotypeMetricAssayTypes:
    DMS = 'DMS'
    EVE = 'EVEscape'


class DefaultGffFeaturesByRegion:
    HA = 'XAJ25415.1'


class LineageSystemNames:
    usda_genoflu = 'usda_genoflu'
    freyja_demixed = 'freyja_demixed'


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


class TableNames:
    samples = 'samples'
    alleles = 'alleles'
    amino_acids = 'amino_acids'
    mutations = 'mutations'
    intra_host_variants = 'intra_host_variants'
    geo_locations = 'geo_locations'
    phenotype_metrics = 'phenotype_metrics'
    phenotype_metric_values = 'phenotype_metric_values'
    lineage_systems = 'lineage_systems'
    lineages = 'lineages'
    samples_lineages = 'samples_lineages'
    translations = 'translations'
    papers = 'papers'
    effects = 'effects'
    annotations = 'annotations'
    annotations_papers = 'annotations_papers'
    annotations_amino_acids = 'annotations_amino_acids'


class StandardColumnNames:
    # ids
    sample_id = 'sample_id'
    allele_id = 'allele_id'
    amino_acid_id = 'amino_acid_id'
    intra_host_variant_id = 'intra_host_variant_id'
    mutation_id = 'mutation_id'
    translation_id = 'translation_id'
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
    consent_level = 'consent_level'
    assay_type = 'assay_type'
    avg_spot_length = 'avg_spot_length'
    bases = 'bases'
    bytes = 'bytes'
    datastore_filetype = 'datastore_filetype'
    datastore_region = 'datastore_region'
    datastore_provider = 'datastore_provider'
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

    # geo locations
    country_name = 'country_name'
    admin1_name = 'admin1_name'
    admin2_name = 'admin2_name'
    admin3_name = 'admin3_name'

    # phenotype metrics
    name = 'name'

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


class StandardPhenoMetricNames:
    species_sera_escape = 'species_sera_escape'
    entry_in_293t_cells = 'entry_in_293t_cells'
    stability = 'stability'
    sa26_usage_increase = 'sa26_usage_increase'
    mature_h5_site = 'mature_h5_site'
    ferret_sera_escape = 'ferret_sera_escape'
    mouse_sera_escape = 'mouse_sera_escape'
    entry_in_sa26_and_sa23_293t_cells = 'entry_in_sa26_and_sa23_293t_cells'


class ConstraintNames:
    uq_intra_host_variants_sample_allele_pair = 'uq_intra_host_variants_sample_allele_pair'
    uq_samples_accession = 'uq_samples_accession'
    uq_mutations_sample_allele_pair = 'uq_mutations_sample_allele_pair'


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
