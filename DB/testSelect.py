import logging
from pprint import pprint

from DB.queries.counts import count_variants_by_column, count_mutations_by_column
from DB.queries.lineages import get_sample_counts_by_lineage
from DB.queries.mutations import get_mutations_by_sample, get_mutations
from DB.queries.prevalence import get_samples_variant_freq_by_aa_change, get_samples_variant_freq_by_nt_change, \
    get_mutation_sample_count_by_aa, get_mutation_sample_count_by_nt, get_pheno_values_and_variant_counts, \
    get_pheno_values_and_mutation_counts
from DB.queries.samples import get_samples_by_mutation, get_samples_by_variant
from DB.queries.variants import get_variants, get_variants_for_sample

logging.basicConfig()
logger = logging.getLogger('sqlalchemy.engine')
logger.setLevel(logging.INFO)

# alleles = get_alleles_via_mutation_by_sample_accession('SRR28752446')
# print(alleles)
#
# accession = 'SRR28752446'
#
# sample_id_query = select(Sample).where(
#     Sample.accession == accession
# ).with_only_columns(Sample.id)
#
# foo = get_variants_for_sample(accession)

# r = count_samples_by_column('region_name')

# r = count_variants_by_column('alt_nt')

# r = count_mutations_by_column('ref_nt')

# r = get_variants_for_sample('region_name = Texas')

# r = get_mutations_by_sample('collection_start_date < 2024-01-01')

# r = get_samples_by_mutation('position_nt < 100')

# r = get_mutations('position_nt > 100 ^ position_nt < 150')

# r = get_variants('position_nt = 100')

# r = get_samples('host = Cat')

# r = get_samples_by_variant('position_aa < 100')

# r = get_samples_variant_freq_by_aa_change('HA:Q238R')

# r = get_samples_variant_freq_by_nt_change('HA:G148-A')

# r = get_pheno_values_and_variant_counts('stability', 'HA', include_refs=False, samples_query=None)
r = get_pheno_values_and_mutation_counts('stability', 'HA', False, None)
# r = get_mutation_sample_count_by_aa('PB2:L502L')

# r = get_mutation_sample_count_by_nt('NA:G696T')

# r = get_sample_counts_by_lineage('host = CAT')

pprint(r)
