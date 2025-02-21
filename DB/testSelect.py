import logging

from sqlalchemy import select
from sqlalchemy.orm import Session, query

from DB.engine import engine
from DB.models import Sample, IntraHostVariant, Allele, AminoAcidSubstitution
from DB.queries.alleles import get_alleles_via_mutation_by_sample_accession
from DB.queries.variants import get_variants_for_sample

logging.basicConfig()
logger = logging.getLogger('sqlalchemy.engine')
logger.setLevel(logging.INFO)

alleles = get_alleles_via_mutation_by_sample_accession('SRR28752446')
print(alleles)

accession = 'SRR28752446'

sample_id_query = select(Sample).where(
    Sample.accession == accession
).with_only_columns(Sample.id)

foo = get_variants_for_sample(accession)

print(foo)
print(len(foo))