from datetime import datetime, date
from typing import List

import sqlalchemy as sa
from sqlalchemy import UniqueConstraint, CheckConstraint, MetaData
from sqlalchemy.ext.asyncio import AsyncAttrs
from sqlalchemy.ext.compiler import compiles
from sqlalchemy.orm import Mapped, mapped_column, DeclarativeBase, relationship

from parser.parser import parser

#########################################################################################
# NOTE:
# Alembic DOES NOT autogenerate check constraints!
# Or, more accurately, it doesn't detect when they've changed. It only creates them when
# it creates a table.
# ==> If you add a check constraint, you must add it to the migration manually! <==
# I've created a little system (or maybe an eldrich horror) that makes this a bit easier:
#
#         for model in [<models with changed check constraints>]:
#             for name, table, sqltext in model.get_check_constraints_for_alembic():
#                 op.execute(f'ALTER TABLE {table} DROP CONSTRAINT IF EXISTS "{name}"')
#                 op.create_check_constraint(name, table, sqltext)
#
# Also, don't use unique=True, it will create an unnamed constraint
#########################################################################################

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


class Base(DeclarativeBase, AsyncAttrs):
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
    r_mutations: Mapped[List['Mutation']] = relationship(back_populates='r_sample')
    r_geo_location: Mapped['GeoLocation'] = relationship(back_populates='r_samples')
    r_sample_lineages: Mapped[List['SampleLineage']] = relationship(back_populates='r_sample')

    def copy_from(self, other: 'Sample'):
        if other.accession != self.accession:
            raise ValueError('Sample accessions do not match, will not copy')

        self.bio_project = other.bio_project
        self.bio_sample = other.bio_sample
        self.bio_sample_accession = other.bio_sample_accession
        self.bio_sample_model = other.bio_sample_model
        self.center_name = other.center_name
        self.experiment = other.experiment
        self.host = other.host
        self.platform = other.platform
        self.instrument = other.instrument
        self.library_layout = other.library_layout
        self.library_name = other.library_name
        self.library_selection = other.library_selection
        self.library_source = other.library_source
        self.organism = other.organism
        self.is_retracted = other.is_retracted
        self.retraction_detected_date = other.retraction_detected_date
        self.isolation_source = other.isolation_source
        self.collection_start_date = other.collection_start_date
        self.collection_end_date = other.collection_end_date
        self.release_date = other.release_date
        self.creation_date = other.creation_date
        self.isolate = other.isolate
        self.version = other.version
        self.sample_name = other.sample_name
        self.sra_study = other.sra_study
        self.serotype = other.serotype
        self.geo_location_id = other.geo_location_id
        self.consent_level = other.consent_level
        self.assay_type = other.assay_type
        self.avg_spot_length = other.avg_spot_length
        self.bases = other.bases
        self.bytes = other.bytes
        self.datastore_filetype = other.datastore_filetype
        self.datastore_region = other.datastore_region
        self.datastore_provider = other.datastore_provider


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

    r_translations: Mapped[List['Translation']] = relationship(back_populates='r_allele')
    r_variants: Mapped[List['IntraHostVariant']] = relationship(back_populates='r_allele')
    r_mutations: Mapped[List['Mutation']] = relationship(back_populates='r_allele')


class AminoAcidSubstitution(Base):
    __tablename__ = 'amino_acid_substitutions'

    id: Mapped[int] = mapped_column(sa.BigInteger, primary_key=True, autoincrement=True)

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
                'position_aa',
                'alt_aa',
                'gff_feature',
                postgresql_nulls_not_distinct=True,
                name='uq_amino_acid_substitutions_aa_values'
            )
        ]
    )

    r_annotations: Mapped[List['Annotation']] = relationship(back_populates='r_amino_sub')
    r_translations: Mapped[List['Translation']] = relationship(back_populates='r_amino_sub')
    r_pheno_measurement_results: Mapped[List['PhenotypeMeasurementResult']] = relationship(back_populates='r_amino_sub')


class Translation(Base):
    __tablename__ = 'translations'

    id: Mapped[int] = mapped_column(sa.BigInteger, primary_key=True, autoincrement=True)

    allele_id: Mapped[int] = mapped_column(sa.ForeignKey('alleles.id'), nullable=False)
    amino_acid_substitution_id: Mapped[int] = mapped_column(
        sa.ForeignKey('amino_acid_substitutions.id'),
        nullable=False,
        index=True
    )

    __table_args__ = tuple(
        [
            UniqueConstraint('allele_id', 'amino_acid_substitution_id', name='uq_translations_allele_and_amino_sub')
        ]
    )

    r_allele: Mapped['Allele'] = relationship(back_populates='r_translations')
    r_amino_sub: Mapped['AminoAcidSubstitution'] = relationship(back_populates='r_translations')


class Mutation(Base):
    __tablename__ = 'mutations'

    id: Mapped[int] = mapped_column(sa.BigInteger, primary_key=True, autoincrement=True)

    sample_id: Mapped[int] = mapped_column(sa.ForeignKey('samples.id'), nullable=False)
    allele_id: Mapped[int] = mapped_column(sa.ForeignKey('alleles.id'), nullable=False, index=True)

    __table_args__ = tuple(
        [
            UniqueConstraint('sample_id', 'allele_id', name='uq_mutations_sample_allele_pair')
        ]
    )

    r_sample: Mapped['Sample'] = relationship(back_populates='r_mutations')
    r_allele: Mapped['Allele'] = relationship(back_populates='r_mutations')


class IntraHostVariant(Base):
    __tablename__ = 'intra_host_variants'

    id: Mapped[int] = mapped_column(sa.BigInteger, primary_key=True, autoincrement=True)

    sample_id: Mapped[int] = mapped_column(sa.ForeignKey('samples.id'), nullable=False)
    allele_id: Mapped[int] = mapped_column(sa.ForeignKey('alleles.id'), nullable=False, index=True)

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

    def copy_from(self, other: 'IntraHostVariant'):
        if not (other.sample_id, other.allele_id) == (self.sample_id, self.allele_id):
            raise ValueError('sample and allele ids do not match, copying will not proceed.')

        self.ref_dp = other.ref_dp
        self.alt_dp = other.alt_dp
        self.alt_freq = other.alt_freq
        self.ref_rv = other.ref_rv
        self.alt_rv = other.alt_rv
        self.ref_qual = other.ref_qual
        self.alt_qual = other.alt_qual
        self.total_dp = other.total_dp
        self.pval = other.pval
        self.pass_qc = other.pass_qc


class GeoLocation(Base):
    __tablename__ = 'geo_locations'
    id: Mapped[int] = mapped_column(sa.BigInteger, primary_key=True, autoincrement=True)

    country_name: Mapped[str] = mapped_column(sa.Text, nullable=False)
    # State / Province / Region
    admin1_name: Mapped[str] = mapped_column(sa.Text, nullable=True)
    # County, Parish, Etc.
    admin2_name: Mapped[str] = mapped_column(sa.Text, nullable=True)
    # Town, locality, etc.
    admin3_name: Mapped[str] = mapped_column(sa.Text, nullable=True)

    geo_center_lon: Mapped[float] = mapped_column(sa.Double, nullable=True)
    geo_center_lat: Mapped[float] = mapped_column(sa.Double, nullable=True)

    __table_args__ = tuple(
        [
            UniqueConstraint(
                'country_name',
                'admin1_name',
                'admin2_name',
                'admin3_name',
                postgresql_nulls_not_distinct=True,
                name='uq_geo_locations_division_names'
            )
        ]
    )

    r_samples: Mapped[List['Sample']] = relationship(back_populates='r_geo_location')


class PhenotypeMetric(Base):
    __tablename__ = 'phenotype_metrics'

    id: Mapped[int] = mapped_column(sa.BigInteger, primary_key=True, autoincrement=True)

    name: Mapped[str] = mapped_column(sa.Text, nullable=False)
    assay_type: Mapped[str] = mapped_column(sa.Text, nullable=False)

    __table_args__ = tuple(
        [
            # todo: could this soften to allow uq name x assay_type?
            UniqueConstraint('name', name='uq_phenotype_metrics_name'),
            CheckConstraint("name <> ''", name='name_not_empty'),
            CheckConstraint("assay_type <> ''", name='assay_type_not_empty')
        ]
    )

    r_pheno_measurement_results: Mapped[List['PhenotypeMeasurementResult']] = relationship(
        back_populates='r_pheno_metric'
    )


class PhenotypeMeasurementResult(Base):
    __tablename__ = 'phenotype_measurement_results'

    id: Mapped[int] = mapped_column(sa.BigInteger, primary_key=True, autoincrement=True)
    phenotype_metric_id: Mapped[int] = mapped_column(sa.ForeignKey('phenotype_metrics.id'), nullable=False)
    amino_acid_substitution_id: Mapped[int] = mapped_column(
        sa.ForeignKey('amino_acid_substitutions.id'),
        nullable=False
    )

    value: Mapped[float] = mapped_column(sa.Double, nullable=False)

    __table_args__ = tuple(
        [
            UniqueConstraint(
                'phenotype_metric_id',
                'amino_acid_substitution_id',
                name='uq_phenotype_measurement_results_metric_and_amino_sub'
            )
        ]
    )

    r_pheno_metric: Mapped['PhenotypeMetric'] = relationship(back_populates='r_pheno_measurement_results')
    r_amino_sub: Mapped['AminoAcidSubstitution'] = relationship(back_populates='r_pheno_measurement_results')


class LineageSystem(Base):
    __tablename__ = 'lineage_systems'
    id: Mapped[int] = mapped_column(sa.BigInteger, primary_key=True, autoincrement=True)
    lineage_system_name: Mapped[str] = mapped_column(sa.Text, nullable=False)

    __table_args__ = tuple(
        [
            UniqueConstraint('lineage_system_name', name='uq_lineage_systems_name'),
        ]
    )

    r_lineages: Mapped[List['Lineage']] = relationship(back_populates='r_lineage_system')


class Lineage(Base):
    __tablename__ = 'lineages'
    id: Mapped[int] = mapped_column(sa.BigInteger, primary_key=True, autoincrement=True)
    lineage_system_id: Mapped[int] = mapped_column(sa.ForeignKey('lineage_systems.id'), nullable=False)
    lineage_name: Mapped[str] = mapped_column(sa.Text, nullable=False)

    __table_args__ = tuple(
        [
            UniqueConstraint('lineage_system_id', 'lineage_name', name='uq_lineages_name_uq_within_system')
        ]
    )

    r_lineage_system: Mapped['LineageSystem'] = relationship(back_populates='r_lineages')
    r_sample_lineages: Mapped[List['SampleLineage']] = relationship(back_populates='r_lineage')


class SampleLineage(Base):
    __tablename__ = 'samples_lineages'
    id: Mapped[int] = mapped_column(sa.BigInteger, primary_key=True, autoincrement=True)

    sample_id: Mapped[int] = mapped_column(sa.ForeignKey('samples.id'), nullable=False)
    lineage_id: Mapped[int] = mapped_column(sa.ForeignKey('lineages.id'), nullable=False, index=True)

    abundance: Mapped[float] = mapped_column(sa.Float, nullable=True)
    is_consensus_call: Mapped[bool] = mapped_column(sa.Boolean, nullable=False)

    __table_args__ = tuple(
        [
            UniqueConstraint(
                'sample_id',
                'lineage_id',
                'is_consensus_call',
                name='uq_samples_lineages_sample_id_lineage_id_is_consensus_call'
                ),
            CheckConstraint('(abundance is null) = is_consensus_call', name='has_abundance_xor_is_consensus')
        ]
    )

    r_lineage: Mapped['Lineage'] = relationship(back_populates='r_sample_lineages')
    r_sample: Mapped['Sample'] = relationship(back_populates='r_sample_lineages')

class Paper(Base):
    __tablename__ = 'papers'

    id: Mapped[int] = mapped_column(sa.BigInteger, primary_key=True, autoincrement=True)

    doi: Mapped[str] = mapped_column(sa.Text, nullable=False)
    author: Mapped[str] = mapped_column(sa.Text, nullable=False)
    publication_year: Mapped[int] = mapped_column(sa.BigInteger, nullable=False)

    __table_args__ = tuple(
        [
            UniqueConstraint('doi', name='uq_title_and_year')
        ]
    )

    r_annotations_papers: Mapped[List['Annotation_Paper']] = relationship(back_populates='r_paper')

class Effect(Base):
    __tablename__ = 'effects'

    id: Mapped[int] = mapped_column(sa.BigInteger, primary_key=True, autoincrement=True)

    detail: Mapped[str] = mapped_column(sa.Text, nullable=False)
    species: Mapped[str] = mapped_column(sa.Text, nullable=True)

    __table_args__ = tuple(
        [
            UniqueConstraint('detail', name='uq_detail')
        ]
    )

    r_annotations: Mapped[List['Annotation']] = relationship(back_populates='r_effect')

class Annotation(Base):
    __tablename__ = 'annotations'

    id: Mapped[int] = mapped_column(sa.BigInteger, primary_key=True, autoincrement=True)

    amino_acid_substitution_id: Mapped[int] = mapped_column(sa.ForeignKey('amino_acid_substitutions.id'), nullable=False)
    effect_id: Mapped[int] = mapped_column(sa.ForeignKey('effects.id'),nullable=False)

    __table_args__ = tuple(
        [
            UniqueConstraint('amino_acid_substitution_id', 'effect_id', name='uq_substitution_and_effect')
        ]
    )

    r_amino_sub: Mapped['AminoAcidSubstitution'] = relationship(back_populates='r_annotations')
    r_annotations_papers: Mapped[List['Annotation_Paper']] = relationship(back_populates='r_annotation')
    r_effect: Mapped['Effect'] = relationship(back_populates='r_annotations')

class Annotation_Paper(Base):
    __tablename__ = 'annotations_papers'

    id: Mapped[int] = mapped_column(sa.BigInteger, primary_key=True, autoincrement=True)

    paper_id: Mapped[int] = mapped_column(sa.ForeignKey('papers.id'), nullable=False)
    annotation_id: Mapped[int] = mapped_column(sa.ForeignKey('annotations.id'), nullable=False)

    __table_args__ = tuple(
        [
            UniqueConstraint('paper_id', 'annotation_id', name='uq_paper_annotation_pair')
        ]
    )

    r_paper: Mapped['Paper'] = relationship(back_populates='r_annotations_papers')
    r_annotation: Mapped['Annotation'] = relationship(back_populates='r_annotations_papers')