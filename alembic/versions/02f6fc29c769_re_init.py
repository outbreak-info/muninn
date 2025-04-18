"""re_init

Revision ID: 02f6fc29c769
Revises: 
Create Date: 2025-03-28 09:22:26.716031

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '02f6fc29c769'
down_revision: Union[str, None] = None
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # ### commands auto generated by Alembic - please adjust! ###
    op.create_table('alleles',
    sa.Column('id', sa.BigInteger(), autoincrement=True, nullable=False),
    sa.Column('region', sa.Text(), nullable=False),
    sa.Column('position_nt', sa.BigInteger(), nullable=False),
    sa.Column('ref_nt', sa.Text(), nullable=False),
    sa.Column('alt_nt', sa.Text(), nullable=False),
    sa.CheckConstraint("alt_nt <> ''", name=op.f('ck_alleles_`alt_nt_not_empty`')),
    sa.CheckConstraint("ref_nt <> ''", name=op.f('ck_alleles_`ref_nt_not_empty`')),
    sa.PrimaryKeyConstraint('id', name=op.f('pk_alleles')),
    sa.UniqueConstraint('region', 'position_nt', 'alt_nt', name='uq_alleles_nt_values', postgresql_nulls_not_distinct=True)
    )
    op.create_table('amino_acid_substitutions',
    sa.Column('id', sa.BigInteger(), autoincrement=True, nullable=False),
    sa.Column('position_aa', sa.BigInteger(), nullable=False),
    sa.Column('ref_aa', sa.Text(), nullable=False),
    sa.Column('alt_aa', sa.Text(), nullable=False),
    sa.Column('ref_codon', sa.Text(), nullable=False),
    sa.Column('alt_codon', sa.Text(), nullable=False),
    sa.Column('gff_feature', sa.Text(), nullable=False),
    sa.CheckConstraint("alt_aa <> ''", name=op.f('ck_amino_acid_substitutions_`alt_aa_not_empty`')),
    sa.CheckConstraint("gff_feature <> ''", name=op.f('ck_amino_acid_substitutions_`gff_feature_not_empty`')),
    sa.CheckConstraint("ref_aa <> ''", name=op.f('ck_amino_acid_substitutions_`ref_aa_not_empty`')),
    sa.PrimaryKeyConstraint('id', name=op.f('pk_amino_acid_substitutions')),
    sa.UniqueConstraint('position_aa', 'alt_aa', 'gff_feature', name='uq_amino_acid_substitutions_aa_values', postgresql_nulls_not_distinct=True)
    )
    op.create_table('geo_locations',
    sa.Column('id', sa.BigInteger(), autoincrement=True, nullable=False),
    sa.Column('full_text', sa.Text(), nullable=False),
    sa.Column('continent_name', sa.Text(), nullable=True),
    sa.Column('country_name', sa.Text(), nullable=True),
    sa.Column('region_name', sa.Text(), nullable=True),
    sa.Column('locality_name', sa.Text(), nullable=True),
    sa.Column('geo_center_lon', sa.Double(), nullable=True),
    sa.Column('geo_center_lat', sa.Double(), nullable=True),
    sa.PrimaryKeyConstraint('id', name=op.f('pk_geo_locations')),
    sa.UniqueConstraint('country_name', 'region_name', 'locality_name', name='uq_geo_locations_country_name_region_name_locality_name', postgresql_nulls_not_distinct=True)
    )
    op.create_table('phenotype_metrics',
    sa.Column('id', sa.BigInteger(), autoincrement=True, nullable=False),
    sa.Column('name', sa.Text(), nullable=False),
    sa.Column('assay_type', sa.Text(), nullable=False),
    sa.CheckConstraint("assay_type <> ''", name=op.f('ck_phenotype_metrics_`assay_type_not_empty`')),
    sa.CheckConstraint("name <> ''", name=op.f('ck_phenotype_metrics_`name_not_empty`')),
    sa.PrimaryKeyConstraint('id', name=op.f('pk_phenotype_metrics')),
    sa.UniqueConstraint('name', name='uq_phenotype_metrics_name')
    )
    op.create_table('phenotype_measurement_results',
    sa.Column('id', sa.BigInteger(), autoincrement=True, nullable=False),
    sa.Column('phenotype_metric_id', sa.BigInteger(), nullable=False),
    sa.Column('amino_acid_substitution_id', sa.BigInteger(), nullable=False),
    sa.Column('value', sa.Double(), nullable=False),
    sa.ForeignKeyConstraint(['amino_acid_substitution_id'], ['amino_acid_substitutions.id'], name=op.f('fk_phenotype_measurement_results_amino_acid_substitution_id_amino_acid_substitutions')),
    sa.ForeignKeyConstraint(['phenotype_metric_id'], ['phenotype_metrics.id'], name=op.f('fk_phenotype_measurement_results_phenotype_metric_id_phenotype_metrics')),
    sa.PrimaryKeyConstraint('id', name=op.f('pk_phenotype_measurement_results')),
    sa.UniqueConstraint('phenotype_metric_id', 'amino_acid_substitution_id', name='uq_phenotype_measurement_results_metric_and_amino_sub')
    )
    op.create_table('samples',
    sa.Column('id', sa.BigInteger(), autoincrement=True, nullable=False),
    sa.Column('accession', sa.Text(), nullable=False),
    sa.Column('bio_project', sa.Text(), nullable=False),
    sa.Column('bio_sample', sa.Text(), nullable=True),
    sa.Column('bio_sample_accession', sa.Text(), nullable=True),
    sa.Column('bio_sample_model', sa.Text(), nullable=False),
    sa.Column('center_name', sa.Text(), nullable=False),
    sa.Column('experiment', sa.Text(), nullable=False),
    sa.Column('host', sa.Text(), nullable=True),
    sa.Column('instrument', sa.Text(), nullable=False),
    sa.Column('platform', sa.Text(), nullable=False),
    sa.Column('isolate', sa.Text(), nullable=True),
    sa.Column('library_name', sa.Text(), nullable=False),
    sa.Column('library_layout', sa.Text(), nullable=False),
    sa.Column('library_selection', sa.Text(), nullable=False),
    sa.Column('library_source', sa.Text(), nullable=False),
    sa.Column('organism', sa.Text(), nullable=False),
    sa.Column('is_retracted', sa.Boolean(), nullable=False),
    sa.Column('retraction_detected_date', sa.TIMESTAMP(timezone=True), nullable=True),
    sa.Column('isolation_source', sa.Text(), nullable=True),
    sa.Column('collection_start_date', sa.Date(), nullable=True),
    sa.Column('collection_end_date', sa.Date(), nullable=True),
    sa.Column('release_date', sa.TIMESTAMP(timezone=True), nullable=False),
    sa.Column('creation_date', sa.TIMESTAMP(timezone=True), nullable=False),
    sa.Column('version', sa.Text(), nullable=False),
    sa.Column('sample_name', sa.Text(), nullable=False),
    sa.Column('sra_study', sa.Text(), nullable=False),
    sa.Column('serotype', sa.Text(), nullable=True),
    sa.Column('geo_location_id', sa.BigInteger(), nullable=True),
    sa.Column('consent_level', sa.Text(), nullable=False),
    sa.Column('assay_type', sa.Text(), nullable=False),
    sa.Column('avg_spot_length', sa.Double(), nullable=True),
    sa.Column('bases', sa.BigInteger(), nullable=False),
    sa.Column('bytes', sa.BigInteger(), nullable=False),
    sa.Column('datastore_filetype', sa.Text(), nullable=False),
    sa.Column('datastore_region', sa.Text(), nullable=False),
    sa.Column('datastore_provider', sa.Text(), nullable=False),
    sa.CheckConstraint('(not is_retracted and retraction_detected_date is null) or (is_retracted and retraction_detected_date is not null)', name=op.f('ck_samples_`retraction_values_existence_in_harmony`')),
    sa.CheckConstraint('collection_start_date <= collection_end_date', name=op.f('ck_samples_`collection_start_not_after_collection_end`')),
    sa.CheckConstraint('num_nulls(collection_start_date, collection_end_date) in (0, 2)', name=op.f('ck_samples_`collection_start_and_end_both_absent_or_both_present`')),
    sa.ForeignKeyConstraint(['geo_location_id'], ['geo_locations.id'], name=op.f('fk_samples_geo_location_id_geo_locations')),
    sa.PrimaryKeyConstraint('id', name=op.f('pk_samples'))
    )
    op.create_table('translations',
    sa.Column('id', sa.BigInteger(), autoincrement=True, nullable=False),
    sa.Column('allele_id', sa.BigInteger(), nullable=False),
    sa.Column('amino_acid_substitution_id', sa.BigInteger(), nullable=False),
    sa.ForeignKeyConstraint(['allele_id'], ['alleles.id'], name=op.f('fk_translations_allele_id_alleles')),
    sa.ForeignKeyConstraint(['amino_acid_substitution_id'], ['amino_acid_substitutions.id'], name=op.f('fk_translations_amino_acid_substitution_id_amino_acid_substitutions')),
    sa.PrimaryKeyConstraint('id', name=op.f('pk_translations')),
    sa.UniqueConstraint('allele_id', 'amino_acid_substitution_id', name='uq_translations_allele_and_amino_sub')
    )
    op.create_table('intra_host_variants',
    sa.Column('id', sa.BigInteger(), autoincrement=True, nullable=False),
    sa.Column('sample_id', sa.BigInteger(), nullable=False),
    sa.Column('allele_id', sa.BigInteger(), nullable=False),
    sa.Column('ref_dp', sa.BigInteger(), nullable=False),
    sa.Column('alt_dp', sa.BigInteger(), nullable=False),
    sa.Column('alt_freq', sa.Double(), nullable=False),
    sa.Column('ref_rv', sa.BigInteger(), nullable=False),
    sa.Column('alt_rv', sa.BigInteger(), nullable=False),
    sa.Column('ref_qual', sa.BigInteger(), nullable=False),
    sa.Column('alt_qual', sa.BigInteger(), nullable=False),
    sa.Column('total_dp', sa.BigInteger(), nullable=False),
    sa.Column('pval', sa.Double(), nullable=False),
    sa.Column('pass_qc', sa.Boolean(), nullable=False),
    sa.ForeignKeyConstraint(['allele_id'], ['alleles.id'], name=op.f('fk_intra_host_variants_allele_id_alleles')),
    sa.ForeignKeyConstraint(['sample_id'], ['samples.id'], name=op.f('fk_intra_host_variants_sample_id_samples')),
    sa.PrimaryKeyConstraint('id', name=op.f('pk_intra_host_variants')),
    sa.UniqueConstraint('sample_id', 'allele_id', name='uq_intra_host_variants_sample_allele_pair')
    )
    op.create_table('mutations',
    sa.Column('id', sa.BigInteger(), autoincrement=True, nullable=False),
    sa.Column('sample_id', sa.BigInteger(), nullable=False),
    sa.Column('allele_id', sa.BigInteger(), nullable=False),
    sa.ForeignKeyConstraint(['allele_id'], ['alleles.id'], name=op.f('fk_mutations_allele_id_alleles')),
    sa.ForeignKeyConstraint(['sample_id'], ['samples.id'], name=op.f('fk_mutations_sample_id_samples')),
    sa.PrimaryKeyConstraint('id', name=op.f('pk_mutations')),
    sa.UniqueConstraint('sample_id', 'allele_id', name='uq_mutations_sample_allele_pair')
    )
    # ### end Alembic commands ###


def downgrade() -> None:
    # ### commands auto generated by Alembic - please adjust! ###
    op.drop_table('mutations')
    op.drop_table('intra_host_variants')
    op.drop_table('translations')
    op.drop_table('samples')
    op.drop_table('phenotype_measurement_results')
    op.drop_table('phenotype_metrics')
    op.drop_table('geo_locations')
    op.drop_table('amino_acid_substitutions')
    op.drop_table('alleles')
    # ### end Alembic commands ###
