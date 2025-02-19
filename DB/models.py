from typing import List

import sqlalchemy as sa
from sqlalchemy import UniqueConstraint, CheckConstraint, MetaData
from sqlalchemy.ext.compiler import compiles
from sqlalchemy.orm import Mapped, mapped_column, relationship, DeclarativeBase

# This is magic and I don't understand it at all.
# From https://stackoverflow.com/a/77475375
UniqueConstraint.argument_for("postgresql", 'nulls_not_distinct', None)


@compiles(UniqueConstraint, "postgresql")
def compile_create_uc(create, compiler, **kw):
    """Add NULLS NOT DISTINCT if its in args."""
    stmt = compiler.visit_unique_constraint(create, **kw)
    postgresql_opts = create.dialect_options["postgresql"]

    if postgresql_opts.get("nulls_not_distinct"):
        return stmt.rstrip().replace("UNIQUE (", "UNIQUE NULLS NOT DISTINCT (")
    return stmt


class BaseModel(DeclarativeBase):
    metadata = MetaData(
        # This will automatically name constraints, but it's still best to name them manually
        # It's possible to get conflicting names from this convention
        naming_convention={
            "ix": "ix_%(column_0_label)s",
            "uq": "uq_%(table_name)s_%(column_0_name)s",
            # you always have to name check constraints, they just get a prefix
            "ck": "ck_%(table_name)s_`%(constraint_name)s`",
            "fk": "fk_%(table_name)s_%(column_0_name)s_%(referred_table_name)s",
            "pk": "pk_%(table_name)s"
        }
    )


class Sample(BaseModel):
    __tablename__ = 'samples'

    id: Mapped[int] = mapped_column(sa.BigInteger, primary_key=True, autoincrement=True)
    accession: Mapped[str] = mapped_column(sa.Text, nullable=False)
    consent_level: Mapped[str] = mapped_column(sa.Text, nullable=False)

    related_intra_host_variants: Mapped[List['IntraHostVariant']] = relationship(back_populates='related_sample')

    # alleles_related_via_intra_host_variants: Mapped[List['Allele']] = relationship()
    # todo: rename to be less confusing, double check that I'm doing it right.
    alleles_related_via_mutation: Mapped[List['Mutation']] = relationship(back_populates='related_sample')


class Allele(BaseModel):
    __tablename__ = 'alleles'

    id: Mapped[int] = mapped_column(sa.BigInteger, primary_key=True, autoincrement=True)

    region: Mapped[str] = mapped_column(sa.Text, nullable=False)
    position_nt: Mapped[int] = mapped_column(sa.BigInteger, nullable=False)
    alt_nt: Mapped[str] = mapped_column(sa.Text, nullable=False)

    __table_args__ = tuple(
        [
            UniqueConstraint(
                'region',
                'position_nt',
                'alt_nt',
                postgresql_nulls_not_distinct=True,
                name='uq_alleles_nt_values'
            ),
            CheckConstraint("alt_nt <> ''", name='alt_nt_not_empty')
        ]
    )

    samples_related_via_mutation: Mapped[List['Mutation']] = relationship(back_populates='related_allele')
    related_intra_host_variants: Mapped[List['IntraHostVariant']] = relationship(back_populates='related_allele')
    related_amino_acid_substitutions: Mapped[List['AminoAcidSubstitution']] = relationship(
        back_populates='related_allele'
    )


class AminoAcidSubstitution(BaseModel):
    __tablename__ = 'amino_acid_substitutions'

    id: Mapped[int] = mapped_column(sa.BigInteger, primary_key=True, autoincrement=True)

    allele_id: Mapped[int] = mapped_column(sa.ForeignKey('alleles.id'), nullable=False)

    position_aa: Mapped[int] = mapped_column(sa.BigInteger, nullable=False)
    ref_aa: Mapped[str] = mapped_column(sa.Text, nullable=False)
    alt_aa: Mapped[str] = mapped_column(sa.Text, nullable=False)
    gff_feature: Mapped[str] = mapped_column(sa.Text, nullable=False)

    __table_args__ = tuple(
        [
            CheckConstraint("gff_feature <> ''", name='gff_feature_not_empty'),
            CheckConstraint("ref_aa <> ''", name='ref_aa_not_empty'),
            CheckConstraint("alt_aa <> ''", name='alt_aa_not_empty'),
            UniqueConstraint(
                'allele_id',
                'position_aa',
                'alt_aa',
                'gff_feature',
                postgresql_nulls_not_distinct=True,
                name='uq_amino_acid_substitutions_aa_values'
            )
        ]
    )

    related_allele: Mapped['Allele'] = relationship(back_populates='related_amino_acid_substitutions')


class Mutation(BaseModel):
    __tablename__ = 'mutations'

    id: Mapped[int] = mapped_column(sa.BigInteger, primary_key=True, autoincrement=True)

    sample_id: Mapped[int] = mapped_column(sa.ForeignKey('samples.id'), nullable=False)
    allele_id: Mapped[int] = mapped_column(sa.ForeignKey('alleles.id'), nullable=False)

    __table_args__ = tuple(
        [
            UniqueConstraint('sample_id', 'allele_id', name='uq_mutations_sample_allele_pair')
        ]
    )

    related_sample: Mapped['Sample'] = relationship(back_populates='alleles_related_via_mutation')
    related_allele: Mapped['Allele'] = relationship(back_populates='samples_related_via_mutation')


class IntraHostVariant(BaseModel):
    __tablename__ = 'intra_host_variants'

    id: Mapped[int] = mapped_column(sa.BigInteger, primary_key=True, autoincrement=True)

    sample_id: Mapped[int] = mapped_column(sa.ForeignKey('samples.id'), nullable=False)
    allele_id: Mapped[int] = mapped_column(sa.ForeignKey('alleles.id'), nullable=False)

    ref_dp: Mapped[int] = mapped_column(sa.BigInteger, nullable=False)
    alt_dp: Mapped[int] = mapped_column(sa.BigInteger, nullable=False)
    alt_freq: Mapped[float] = mapped_column(sa.Double, nullable=False)

    __table_args__ = tuple(
        [
            UniqueConstraint('sample_id', 'allele_id', name='uq_intra_host_variants_sample_allele_pair')
        ]
    )

    related_sample: Mapped['Sample'] = relationship(back_populates='related_intra_host_variants')
    related_allele: Mapped['Allele'] = relationship(back_populates='related_intra_host_variants')


class DmsResult(BaseModel):
    __tablename__ = 'dms_results'

    id: Mapped[int] = mapped_column(sa.BigInteger, primary_key=True, autoincrement=True)
    ferret_sera_escape: Mapped[float] = mapped_column(sa.Double, nullable=False)
    stability: Mapped[float] = sa.Column(sa.Double, nullable=False)

    allele_id: Mapped[int] = mapped_column(sa.ForeignKey('alleles.id'), nullable=False)

    __table_args__ = tuple(
        [
            UniqueConstraint('allele_id')
        ]
    )
