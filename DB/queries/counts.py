from sqlalchemy import text, select
from sqlalchemy.orm import Session
from sqlalchemy.sql.functions import count, func

from DB.engine import engine
from DB.models import Sample, Base, GeoLocation, IntraHostVariant, AminoAcidSubstitution, Allele, Mutation


# unsure how useful this will be, most tables may need bespoke joins.
def count_x_by_y(x: 'Base', y: str):
    with Session(engine) as session:
        res = session.execute(
            select(x)
            .select_from(x)
            .with_only_columns(text(y), func.count().label('count1'))
            .group_by(text(y))
            .order_by(text('count1 desc'))
        ).all()
        return res


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
            select(IntraHostVariant, Allele, AminoAcidSubstitution)
            .join(Allele, Allele.id == IntraHostVariant.allele_id, isouter=True)
            .join(AminoAcidSubstitution, AminoAcidSubstitution.allele_id == Allele.id, isouter=True)
            .with_only_columns(text(by_col), func.count().label('count1'))
            .group_by(text(by_col))
            .order_by(text('count1 desc'))
        ).all()
        return res


def count_mutations_by_column(by_col: str):
    with Session(engine) as session:
        res = session.execute(
            select(Mutation, Allele, AminoAcidSubstitution)
            .join(Allele, Allele.id == Mutation.allele_id, isouter=True)
            .join(AminoAcidSubstitution, AminoAcidSubstitution.allele_id == Allele.id, isouter=True)
            .with_only_columns(text(by_col), func.count().label('count1'))
            .group_by(text(by_col))
            .order_by(text('count1 desc'))
        ).all()
        return res

