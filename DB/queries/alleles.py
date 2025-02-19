from typing import List

from sqlalchemy import select
from sqlalchemy.orm import Session

from DB.engine import engine
from DB.models import Allele, Sample, Mutation, IntraHostVariant


def get_allele_by_region_position_alt(region: str, position: int, alt_nt: str) -> 'Allele':
    with Session(engine) as session:
        return session.execute(
            select(Allele).where(
                Allele.region == region,
                Allele.position_nt == position,
                Allele.alt_nt == alt_nt
            )
        ).scalar()


def get_alleles_via_mutation_by_sample_accession(accession: str) -> List['Allele']:
    sample_id_query = select(Sample).where(
        Sample.accession == accession
    ).with_only_columns(Sample.id)

    mutations_query = select(Mutation).filter(
        Mutation.sample_id.in_(sample_id_query)
    ).with_only_columns(Mutation.allele_id)

    alleles_query = select(Allele).filter(Allele.id.in_(mutations_query))

    with (Session(engine) as session):
        alleles = session.execute(alleles_query).scalars()
        return [a for a in alleles]


def get_alleles_via_intra_host_variant_by_sample_accession(accession: str) -> list['Allele']:
    sample_id_query = select(Sample).where(
        Sample.accession == accession
    ).with_only_columns(Sample.id)

    ihv_query = select(IntraHostVariant).filter(
        IntraHostVariant.sample_id.in_(sample_id_query)
    ).with_only_columns(IntraHostVariant.allele_id)

    alleles_query = select(Allele).filter(Allele.id.in_(ihv_query))

    with (Session(engine) as session):
        alleles = session.execute(alleles_query).scalars()
        return [a for a in alleles]
