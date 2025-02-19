import logging

from DB.queries.alleles import get_alleles_via_mutation_by_sample_accession, \
    get_alleles_via_intra_host_variant_by_sample_accession, get_allele_by_region_position_alt
from DB.queries.amino_acid_substitutions import get_aa_subs_via_mutation_by_sample_accession
from DB.queries.samples import get_samples_via_mutation_by_allele_id

logging.basicConfig()
logger = logging.getLogger('sqlalchemy.engine')
logger.setLevel(logging.INFO)

alleles = get_alleles_via_mutation_by_sample_accession('SRR28752446')
print(alleles)

alleles2 = get_alleles_via_intra_host_variant_by_sample_accession('SRR28752446')
print(alleles2)

start_allele = get_allele_by_region_position_alt('NS', 760,'A')
samples = get_samples_via_mutation_by_allele_id(start_allele.id)
print(samples)


aa_subs = get_aa_subs_via_mutation_by_sample_accession('SRR28752446')
print(aa_subs)