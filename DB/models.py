from datetime import datetime
from typing import List

import sqlalchemy as sa
from sqlalchemy import UniqueConstraint, CheckConstraint, MetaData
from sqlalchemy.ext.compiler import compiles
from sqlalchemy.orm import Mapped, mapped_column, DeclarativeBase, relationship

from api.models import PydAminoAcidSubstitution

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


class Base(DeclarativeBase):
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


class Sample(Base):
    __tablename__ = 'samples'

    id: Mapped[int] = mapped_column(sa.BigInteger, primary_key=True, autoincrement=True)
    # todo: change name to be specific about which accession this is
    accession: Mapped[str] = mapped_column(sa.Text, nullable=False)
    bio_project: Mapped[str] = mapped_column(sa.Text, nullable=False)
    bio_sample: Mapped[str] = mapped_column(sa.Text, nullable=False)
    bio_sample_accession: Mapped[str] = mapped_column(sa.Text, nullable=True)
    bio_sample_model: Mapped[str] = mapped_column(sa.Text, nullable=False)
    center_name: Mapped[str] = mapped_column(sa.Text, nullable=False)
    experiment: Mapped[str] = mapped_column(sa.Text, nullable=False)

    # todo: host should benefit from some normalization
    # we've got various spellings of common names, plus some binomials and genus sp.
    host: Mapped[str] = mapped_column(sa.Text, nullable=False)

    # todo: could these be relationships?
    instrument: Mapped[str] = mapped_column(sa.Text, nullable=False)
    platform: Mapped[str] = mapped_column(sa.Text, nullable=False)

    isolate: Mapped[str] = mapped_column(sa.Text, nullable=False)

    #todo: factor these out?
    library_name: Mapped[str] = mapped_column(sa.Text, nullable=False)
    library_layout: Mapped[str] = mapped_column(sa.Text, nullable=False)
    library_selection: Mapped[str] = mapped_column(sa.Text, nullable=False)
    library_source: Mapped[str] = mapped_column(sa.Text, nullable=False)
    # todo: right now, this is all 'Influenza A virus'
    organism: Mapped[str] = mapped_column(sa.Text, nullable=False)

    # todo: if it is retracted it needs a date? and v/v?
    is_retracted: Mapped[bool] = mapped_column(sa.Boolean, nullable=False)
    # todo: this needs to come with its tz data attached
    retraction_detected_date: Mapped[datetime] = mapped_column(sa.DateTime, nullable=True)

    # todo: should have some normalization
    isolation_source: Mapped[str] = mapped_column(sa.Text, nullable=True)

    # todo: this is a messy field
    # need to have nulls, year only, and some have multiple dates listed
    # also deal with "missing" vs just null
    # we could store it as a string, but then we'll lose out on postgres queries using the date type
    # idea: store the raw string, and a 'searchable date' that's the pg type?
    collection_date: Mapped[str] = mapped_column(sa.Text, nullable=True)

    # these date fields aren't as messy
    release_date: Mapped[str] = mapped_column(sa.Text, nullable=False)
    creation_date: Mapped[str] = mapped_column(sa.Text, nullable=False)

    # todo: What is this? all = 1 in the file I have
    version: Mapped[str] = mapped_column(sa.Text, nullable=False)

    sample_name: Mapped[str] = mapped_column(sa.Text, nullable=False)
    sra_study: Mapped[str] = mapped_column(sa.Text, nullable=False)

    serotype: Mapped[str] = mapped_column(sa.Text, nullable=True)


    # todo: these are a mess
    geo_loc_name: Mapped[str] = mapped_column(sa.Text, nullable=True)
    geo_loc_name_country: Mapped[str] = mapped_column(sa.Text, nullable=True)
    geo_loc_name_country_continent: Mapped[str] = mapped_column(sa.Text, nullable=True)

    consent_level: Mapped[str] = mapped_column(sa.Text, nullable=False)
    assay_type: Mapped[str] = mapped_column(sa.Text, nullable=False)
    avg_spot_length: Mapped[float] = mapped_column(sa.Double, nullable=False)
    bases: Mapped[int] = mapped_column(sa.BigInteger, nullable=False)
    bytes: Mapped[int] = mapped_column(sa.BigInteger, nullable=False)

    # todo: I think these could maybe be factored out and stored differently?
    datastore_filetype: Mapped[str] = mapped_column(sa.Text, nullable=False)
    datastore_region: Mapped[str] = mapped_column(sa.Text, nullable=False)
    datastore_provider: Mapped[str] = mapped_column(sa.Text, nullable=False)


    r_variants: Mapped[List['IntraHostVariant']] = relationship(back_populates='r_sample')


class Allele(Base):
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

    r_amino_subs: Mapped[List['AminoAcidSubstitution']] = relationship(back_populates='r_allele')
    r_variants: Mapped[List['IntraHostVariant']] = relationship(back_populates='r_allele')


class AminoAcidSubstitution(Base):
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

    r_allele: Mapped['Allele'] = relationship(back_populates='r_amino_subs')

    def to_pyd_model(self) -> 'PydAminoAcidSubstitution':
        return PydAminoAcidSubstitution(
            id=self.id,
            allele_id=self.allele_id,
            position_aa=self.position_aa,
            ref_aa=self.ref_aa,
            alt_aa=self.alt_aa,
            gff_feature=self.gff_feature
        )


class Mutation(Base):
    __tablename__ = 'mutations'

    id: Mapped[int] = mapped_column(sa.BigInteger, primary_key=True, autoincrement=True)

    sample_id: Mapped[int] = mapped_column(sa.ForeignKey('samples.id'), nullable=False)
    allele_id: Mapped[int] = mapped_column(sa.ForeignKey('alleles.id'), nullable=False)

    __table_args__ = tuple(
        [
            UniqueConstraint('sample_id', 'allele_id', name='uq_mutations_sample_allele_pair')
        ]
    )


class IntraHostVariant(Base):
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

    r_sample: Mapped['Sample'] = relationship(back_populates='r_variants')
    r_allele: Mapped['Allele'] = relationship(back_populates='r_variants')


class DmsResult(Base):
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
