from typing import List

from sqlalchemy import select, text
from sqlalchemy.orm import Session, contains_eager

from DB.engine import engine
from DB.models import Sample, IntraHostVariant, Allele, AminoAcidSubstitution, GeoLocation, Translation
from api.models import VariantInfo
from parser.parser import parser


def get_variants(query: str) -> List['VariantInfo']:
    # todo: bind parameters
    user_query = parser.parse(query)

    variants_query = (
        select(IntraHostVariant, Allele, Translation, AminoAcidSubstitution)
        .join(Allele, IntraHostVariant.allele_id == Allele.id, isouter=True)
        .options(contains_eager(IntraHostVariant.r_allele))
        .join(Translation, Allele.id == Translation.allele_id, isouter=True)
        .options(contains_eager(Allele.r_translations))
        .join(AminoAcidSubstitution, Translation.amino_acid_substitution_id == AminoAcidSubstitution.id, isouter=True)
        .options(contains_eager(Translation.r_amino_sub))
        .where(text(user_query))
    )

    with Session(engine) as session:
        variants = session.execute(variants_query).unique().scalars()
        out_data = [VariantInfo.from_db_object(v) for v in variants]
    return out_data



def get_variants_for_sample(query: str) -> List['VariantInfo']:
    user_query = parser.parse(query)
    variants_query = (
        select(IntraHostVariant, Allele, Translation, AminoAcidSubstitution)
        .join(Allele, IntraHostVariant.allele_id == Allele.id, isouter=True)
        .options(contains_eager(IntraHostVariant.r_allele))
        .join(Translation, Allele.id == Translation.allele_id, isouter=True)
        .options(contains_eager(Allele.r_translations))
        .join(AminoAcidSubstitution, Translation.amino_acid_substitution_id == AminoAcidSubstitution.id, isouter=True)
        .options(contains_eager(Translation.r_amino_sub))
        .filter(
            # todo: bind parameters
            IntraHostVariant.sample_id.in_(
                select(Sample.id)
                .join(GeoLocation, GeoLocation.id == Sample.geo_location_id, isouter=True)
                .where(text(user_query))
            )
        )
    )

    with (Session(engine) as session):
        results = session.execute(variants_query).unique().scalars()
        out_data = [VariantInfo.from_db_object(v) for v in results]
    return out_data
