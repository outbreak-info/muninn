from datetime import datetime, date
from typing import List, Tuple

from alembic import op
import sqlalchemy as sa
from sqlalchemy import UniqueConstraint, CheckConstraint, MetaData
from sqlalchemy.ext.compiler import compiles
from sqlalchemy.orm import Mapped, mapped_column, DeclarativeBase, relationship

from api.models import PydAminoAcidSubstitution
from parser.parser import parser

############################################################################
# NOTE:
# Alembic DOES NOT autogenerate check constraints!
# If you add a check constraint, you must add it to the migration manually!
# I've created a little system (or maybe an eldrich horror) that makes this a bit easier
# Also, don't use unique=True, it will create an unnamed constraint
############################################################################


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

    # todo: I think this might be a terrible idea...
    @classmethod
    def get_check_constraints_for_alembic(cls) -> List[tuple[str, str, str]]:
        checks = []
        for arg in cls.__table_args__:
            if type(arg) != CheckConstraint:
                continue
            arg: CheckConstraint

            checks.append((arg.name, cls.__tablename__, arg.sqltext.text))
        return checks

    @classmethod
    def parse_query(cls, querytext):
        q = parser.parse(querytext)
        return f'SELECT {cls.__tablename__}.id FROM {cls.__tablename__} WHERE ({q})'

class Sample(Base):
    __tablename__ = 'samples'

    id: Mapped[int] = mapped_column(sa.BigInteger, primary_key=True, autoincrement=True)
    # todo: change name to be specific about which accession this is
    accession: Mapped[str] = mapped_column(sa.Text, nullable=False)
    bio_project: Mapped[str] = mapped_column(sa.Text, nullable=False)
    bio_sample: Mapped[str] = mapped_column(sa.Text, nullable=True)
    bio_sample_accession: Mapped[str] = mapped_column(sa.Text, nullable=True)
    bio_sample_model: Mapped[str] = mapped_column(sa.Text, nullable=False)
    center_name: Mapped[str] = mapped_column(sa.Text, nullable=False)
    experiment: Mapped[str] = mapped_column(sa.Text, nullable=False)

    # todo: host should benefit from some normalization
    # we've got various spellings of common names, plus some binomials and genus sp.
    host: Mapped[str] = mapped_column(sa.Text, nullable=True)

    # todo: could these be relationships?
    instrument: Mapped[str] = mapped_column(sa.Text, nullable=False)
    platform: Mapped[str] = mapped_column(sa.Text, nullable=False)

    isolate: Mapped[str] = mapped_column(sa.Text, nullable=True)

    # todo: factor these out?
    library_name: Mapped[str] = mapped_column(sa.Text, nullable=False)
    library_layout: Mapped[str] = mapped_column(sa.Text, nullable=False)
    library_selection: Mapped[str] = mapped_column(sa.Text, nullable=False)
    library_source: Mapped[str] = mapped_column(sa.Text, nullable=False)
    # todo: right now, this is all 'Influenza A virus'
    organism: Mapped[str] = mapped_column(sa.Text, nullable=False)

    # todo: if it is retracted it needs a date? and v/v?
    is_retracted: Mapped[bool] = mapped_column(sa.Boolean, nullable=False)
    # todo: this needs to come with its tz data attached (it's utc)
    retraction_detected_date: Mapped[datetime] = mapped_column(sa.TIMESTAMP(timezone=True), nullable=True)

    # todo: should have some normalization
    isolation_source: Mapped[str] = mapped_column(sa.Text, nullable=True)

    # split out from collection date
    collection_start_date: Mapped[date] = mapped_column(sa.Date, nullable=True)
    collection_end_date: Mapped[date] = mapped_column(sa.Date, nullable=True)

    # these date fields aren't as messy
    # todo: release date should come with tz info
    release_date: Mapped[datetime] = mapped_column(sa.TIMESTAMP(timezone=True), nullable=False)
    creation_date: Mapped[datetime] = mapped_column(sa.TIMESTAMP(timezone=True), nullable=False)

    # todo: What is this? all = 1 in the file I have
    version: Mapped[str] = mapped_column(sa.Text, nullable=False)

    sample_name: Mapped[str] = mapped_column(sa.Text, nullable=False)
    sra_study: Mapped[str] = mapped_column(sa.Text, nullable=False)

    serotype: Mapped[str] = mapped_column(sa.Text, nullable=True)

    # from geo_loc_name
    # todo: we are currently ignoring the continent and country fields
    geo_location_id: Mapped[int] = mapped_column(sa.ForeignKey('geo_locations.id'), nullable=True)

    consent_level: Mapped[str] = mapped_column(sa.Text, nullable=False)
    assay_type: Mapped[str] = mapped_column(sa.Text, nullable=False)
    avg_spot_length: Mapped[float] = mapped_column(sa.Double, nullable=True)
    bases: Mapped[int] = mapped_column(sa.BigInteger, nullable=False)
    bytes: Mapped[int] = mapped_column(sa.BigInteger, nullable=False)

    # todo: I think these could maybe be factored out and stored differently?
    datastore_filetype: Mapped[str] = mapped_column(sa.Text, nullable=False)
    datastore_region: Mapped[str] = mapped_column(sa.Text, nullable=False)
    datastore_provider: Mapped[str] = mapped_column(sa.Text, nullable=False)

    __table_args__ = tuple(
        [
            CheckConstraint(
                '(not is_retracted and retraction_detected_date is null) or '
                '(is_retracted and retraction_detected_date is not null)',
                name='retraction_values_existence_in_harmony'
            ),
            CheckConstraint(
                'num_nulls(collection_start_date, collection_end_date) in (0, 2)',
                name='collection_start_and_end_both_absent_or_both_present'
            ),
            CheckConstraint(
                'collection_start_date <= collection_end_date',
                name='collection_start_not_after_collection_end'
            )
        ]
    )

    r_variants: Mapped[List['IntraHostVariant']] = relationship(back_populates='r_sample')
    r_geo_location: Mapped['GeoLocation'] = relationship(back_populates='r_samples')


class Allele(Base):
    __tablename__ = 'alleles'

    id: Mapped[int] = mapped_column(sa.BigInteger, primary_key=True, autoincrement=True)

    region: Mapped[str] = mapped_column(sa.Text, nullable=False)
    # todo: the ihv files have a much longer region string, what do with that?
    # HA|PP755589.1|A/cattle/Texas/24-008749-003/2024(H5N1)
    position_nt: Mapped[int] = mapped_column(sa.BigInteger, nullable=False)
    ref_nt: Mapped[str] = mapped_column(sa.Text, nullable=False)
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
            CheckConstraint("alt_nt <> ''", name='alt_nt_not_empty'),
            CheckConstraint("ref_nt <> ''", name='ref_nt_not_empty')
        ]
    )

    r_amino_subs: Mapped[List['AminoAcidSubstitution']] = relationship(back_populates='r_allele')
    r_variants: Mapped[List['IntraHostVariant']] = relationship(back_populates='r_allele')
    r_dms_results: Mapped[List['DmsResult']] = relationship(back_populates='r_allele')


class AminoAcidSubstitution(Base):
    __tablename__ = 'amino_acid_substitutions'

    id: Mapped[int] = mapped_column(sa.BigInteger, primary_key=True, autoincrement=True)

    allele_id: Mapped[int] = mapped_column(sa.ForeignKey('alleles.id'), nullable=False)

    position_aa: Mapped[int] = mapped_column(sa.BigInteger, nullable=False)
    ref_aa: Mapped[str] = mapped_column(sa.Text, nullable=False)
    alt_aa: Mapped[str] = mapped_column(sa.Text, nullable=False)
    ref_codon: Mapped[str] = mapped_column(sa.Text, nullable=False)
    alt_codon: Mapped[str] = mapped_column(sa.Text, nullable=False)
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
    ref_rv: Mapped[int] = mapped_column(sa.BigInteger, nullable=False)
    alt_rv: Mapped[int] = mapped_column(sa.BigInteger, nullable=False)
    ref_qual: Mapped[int] = mapped_column(sa.BigInteger, nullable=False)
    alt_qual: Mapped[int] = mapped_column(sa.BigInteger, nullable=False)
    total_dp: Mapped[int] = mapped_column(sa.BigInteger, nullable=False)
    pval: Mapped[float] = mapped_column(sa.Double, nullable=False)
    # todo: check name, pass is a keyword in python
    pass_qc: Mapped[bool] = mapped_column(sa.Boolean, nullable=False)

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
    allele_id: Mapped[int] = mapped_column(sa.ForeignKey('alleles.id'), nullable=False)

    # todo: no data for any of these, so I'm guessing at the types
    ferret_sera_escape: Mapped[float] = mapped_column(sa.Double, nullable=False)
    stability: Mapped[float] = mapped_column(sa.Double, nullable=False)
    entry_293T: Mapped[float] = mapped_column(sa.Double, nullable=False)
    SA26_usage_increase: Mapped[float] = mapped_column(sa.Double, nullable=False)

    __table_args__ = tuple(
        [
            UniqueConstraint('allele_id')
        ]
    )

    r_allele: Mapped['Allele'] = relationship(back_populates='r_dms_results')


class GeoLocation(Base):
    __tablename__ = 'geo_locations'
    id: Mapped[int] = mapped_column(sa.BigInteger, primary_key=True, autoincrement=True)

    full_text: Mapped[str] = mapped_column(sa.Text, nullable=False)

    continent_name: Mapped[str] = mapped_column(sa.Text, nullable=True)
    country_name: Mapped[str] = mapped_column(sa.Text, nullable=True)
    region_name: Mapped[str] = mapped_column(sa.Text, nullable=True)
    locality_name: Mapped[str] = mapped_column(sa.Text, nullable=True)

    geo_center_lon: Mapped[float] = mapped_column(sa.Double, nullable=True)
    geo_center_lat: Mapped[float] = mapped_column(sa.Double, nullable=True)

    __table_args__ = tuple(
        [
            UniqueConstraint(
                'country_name',
                'region_name',
                'locality_name',
                postgresql_nulls_not_distinct=True,
                name='uq_geo_locations_country_name_region_name_locality_name'
            )
        ]
    )

    r_samples: Mapped[List['Sample']] = relationship(back_populates='r_geo_location')
