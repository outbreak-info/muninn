"""annotations

Revision ID: 541c861117dd
Revises: dae15b3195bb
Create Date: 2025-07-09 17:06:16.438416

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '541c861117dd'
down_revision: Union[str, None] = 'dae15b3195bb'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # ### commands auto generated by Alembic - please adjust! ###
    op.create_table('effects',
    sa.Column('id', sa.BigInteger(), autoincrement=True, nullable=False),
    sa.Column('detail', sa.Text(), nullable=False),
    sa.PrimaryKeyConstraint('id', name=op.f('pk_effects')),
    sa.UniqueConstraint('detail', name='uq_effects_detail')
    )
    op.create_table('papers',
    sa.Column('id', sa.BigInteger(), autoincrement=True, nullable=False),
    sa.Column('authors', sa.Text(), nullable=False),
    sa.Column('title', sa.Text(), nullable=False),
    sa.Column('publication_year', sa.BigInteger(), nullable=False),
    sa.PrimaryKeyConstraint('id', name=op.f('pk_papers')),
    sa.UniqueConstraint('authors', 'publication_year', 'title', name='uq_papers_authors_title_year')
    )
    op.create_table('annotations',
    sa.Column('id', sa.BigInteger(), autoincrement=True, nullable=False),
    sa.Column('effect_id', sa.BigInteger(), nullable=False),
    sa.ForeignKeyConstraint(['effect_id'], ['effects.id'], name=op.f('fk_annotations_effect_id_effects')),
    sa.PrimaryKeyConstraint('id', name=op.f('pk_annotations'))
    )
    op.create_table('annotations_amino_acids',
    sa.Column('id', sa.BigInteger(), autoincrement=True, nullable=False),
    sa.Column('amino_acid_id', sa.BigInteger(), nullable=False),
    sa.Column('annotation_id', sa.BigInteger(), nullable=False),
    sa.ForeignKeyConstraint(['amino_acid_id'], ['amino_acids.id'], name=op.f('fk_annotations_amino_acids_amino_acid_id_amino_acids')),
    sa.ForeignKeyConstraint(['annotation_id'], ['annotations.id'], name=op.f('fk_annotations_amino_acids_annotation_id_annotations')),
    sa.PrimaryKeyConstraint('id', name=op.f('pk_annotations_amino_acids')),
    sa.UniqueConstraint('amino_acid_id', 'annotation_id', name='uq_annotations_amino_acids_pair')
    )
    op.create_table('annotations_papers',
    sa.Column('id', sa.BigInteger(), autoincrement=True, nullable=False),
    sa.Column('paper_id', sa.BigInteger(), nullable=False),
    sa.Column('annotation_id', sa.BigInteger(), nullable=False),
    sa.Column('quotation', sa.Text(), nullable=True),
    sa.ForeignKeyConstraint(['annotation_id'], ['annotations.id'], name=op.f('fk_annotations_papers_annotation_id_annotations')),
    sa.ForeignKeyConstraint(['paper_id'], ['papers.id'], name=op.f('fk_annotations_papers_paper_id_papers')),
    sa.PrimaryKeyConstraint('id', name=op.f('pk_annotations_papers')),
    sa.UniqueConstraint('paper_id', 'annotation_id', name='uq_annotations_papers_annotation_paper_pair')
    )
    # ### end Alembic commands ###


def downgrade() -> None:
    # ### commands auto generated by Alembic - please adjust! ###
    op.drop_table('annotations_papers')
    op.drop_table('annotations_amino_acids')
    op.drop_table('annotations')
    op.drop_table('papers')
    op.drop_table('effects')
    # ### end Alembic commands ###
