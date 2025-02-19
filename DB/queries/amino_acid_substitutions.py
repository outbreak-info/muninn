from typing import List

from sqlalchemy import select
from sqlalchemy.orm import Session

from DB.engine import engine
from DB.models import AminoAcidSubstitution, Sample, Mutation, Allele


def get_aa_subs_via_mutation_by_sample_accession(accession: str) -> List['AminoAcidSubstitution']:
    sample_id_query = select(Sample).where(
        Sample.accession == accession
    ).with_only_columns(Sample.id)

    mutations_query = select(Mutation).filter(
        Mutation.sample_id.in_(sample_id_query)
    ).with_only_columns(Mutation.id)

    alleles_query = select(Allele).filter(Allele.id.in_(mutations_query)).with_only_columns(Allele.id)

    aa_subs_query = select(AminoAcidSubstitution).filter(AminoAcidSubstitution.allele_id.in_(alleles_query))

    with (Session(engine) as session):
        aa_subs = session.execute(aa_subs_query).scalars()
        return [a for a in aa_subs]