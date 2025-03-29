from sqlalchemy import text, select
from sqlalchemy.orm import Session
from sqlalchemy.sql.functions import func

from DB.engine import engine
from DB.models import Sample, GeoLocation, IntraHostVariant, AminoAcidSubstitution, Allele, Mutation, Translation


def count_samples_by_column(by_col: str):
    with Session(engine) as session:
        res = session.execute(
            select(Sample, GeoLocation)
            .join(GeoLocation, GeoLocation.id == Sample.geo_location_id, isouter=True)
            .select_from(Sample)
            .with_only_columns(text(by_col), func.count().label('count1'))
            .group_by(text(by_col))
            .order_by(text('count1 desc'))
        ).all()
        return res


def count_variants_by_column(by_col: str):
    with Session(engine) as session:
        res = session.execute(
            select(IntraHostVariant, Allele, Translation, AminoAcidSubstitution)
            .join(Allele, Allele.id == IntraHostVariant.allele_id, isouter=True)
            .join(Translation, Allele.id == Translation.allele_id, isouter=True)
            .join(
                AminoAcidSubstitution,
                Translation.amino_acid_substitution_id == AminoAcidSubstitution.id,
                isouter=True
            )
            .with_only_columns(text(by_col), func.count().label('count1'))
            .group_by(text(by_col))
            .order_by(text('count1 desc'))
        ).all()
        return res


def count_mutations_by_column(by_col: str):
    with Session(engine) as session:
        res = session.execute(
            select(Mutation, Allele, Translation, AminoAcidSubstitution)
            .join(Allele, Allele.id == Mutation.allele_id, isouter=True)
            .join(Translation, Allele.id == Translation.allele_id, isouter=True)
            .join(
                AminoAcidSubstitution,
                Translation.amino_acid_substitution_id == AminoAcidSubstitution.id,
                isouter=True
            )
            .with_only_columns(text(by_col), func.count().label('count1'))
            .group_by(text(by_col))
            .order_by(text('count1 desc'))
        ).all()
        return res
