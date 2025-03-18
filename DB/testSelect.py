import logging

from sqlalchemy import select
from sqlalchemy.orm import Session, query

from DB.engine import engine
from DB.models import Sample, IntraHostVariant, Allele, AminoAcidSubstitution
from DB.queries.alleles import get_alleles_via_mutation_by_sample_accession
from DB.queries.counts import count_samples_by_column, count_variants_by_column, count_mutations_by_column
from DB.queries.samples import get_samples_by_mutation
from DB.queries.variants import get_variants_for_sample

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


r = count_samples_by_column('region_name')
print(r)

r = count_variants_by_column('alt_nt')
print(r)

r = count_mutations_by_column('ref_nt')
print(r)