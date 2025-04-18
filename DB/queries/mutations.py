from typing import List

from sqlalchemy import select, text
from sqlalchemy.orm import Session, contains_eager

from DB.engine import engine
from DB.models import Mutation, Allele, AminoAcidSubstitution, Sample, GeoLocation, Translation
from api.models import MutationInfo
from parser.parser import parser


def get_mutations(query: str) -> List['MutationInfo']:
    user_query = parser.parse(query)

    mutations_query = (
        select(Mutation, Allele, Translation, AminoAcidSubstitution)
        .join(Allele, Mutation.allele_id == Allele.id, isouter=True)
        .options(contains_eager(Mutation.r_allele))
        .join(Translation, Allele.id == Translation.allele_id, isouter=True)
        .options(contains_eager(Allele.r_translations))
        .join(AminoAcidSubstitution, Translation.amino_acid_substitution_id == AminoAcidSubstitution.id, isouter=True)
        .options(contains_eager(Translation.r_amino_sub))
        .where(
            text(user_query)
        )
    )

    with Session(engine) as session:
        results = session.execute(mutations_query).unique().scalars()
        out_data = [MutationInfo.from_db_object(m) for m in results]
    return out_data


def get_mutations_by_sample(query: str) -> List['MutationInfo']:
    # todo: bind parameters
    user_query = parser.parse(query)

    mutations_query = (
        select(Mutation, Allele, Translation, AminoAcidSubstitution)
        .join(Allele, Mutation.allele_id == Allele.id, isouter=True)
        .options(contains_eager(Mutation.r_allele))
        .join(Translation, Allele.id == Translation.allele_id, isouter=True)
        .options(contains_eager(Allele.r_translations))
        .join(AminoAcidSubstitution, Translation.amino_acid_substitution_id == AminoAcidSubstitution.id, isouter=True)
        .options(contains_eager(Translation.r_amino_sub))
        .where(
            Mutation.sample_id.in_(
                select(Sample.id)
                .join(GeoLocation, GeoLocation.id == Sample.geo_location_id, isouter=True)
                .where(text(user_query))
            )
        )
    )

    with Session(engine) as session:
        results = session.execute(mutations_query).unique().scalars()
        out_data = [MutationInfo.from_db_object(m) for m in results]
    return out_data
