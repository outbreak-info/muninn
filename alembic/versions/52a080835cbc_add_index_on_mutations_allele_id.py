"""add_index_on_mutations_allele_id

Revision ID: 52a080835cbc
Revises: b8ad6e225ead
Create Date: 2025-04-08 16:43:04.652601

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '52a080835cbc'
down_revision: Union[str, None] = 'b8ad6e225ead'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # ### commands auto generated by Alembic - please adjust! ###
    op.drop_index('allele_id_idx', table_name='intra_host_variants')
    op.create_index(op.f('ix_mutations_allele_id'), 'mutations', ['allele_id'], unique=False)
    op.drop_index('amino_acid_sub_id_idx', table_name='translations')
    # ### end Alembic commands ###


def downgrade() -> None:
    # ### commands auto generated by Alembic - please adjust! ###
    op.create_index('amino_acid_sub_id_idx', 'translations', ['amino_acid_substitution_id'], unique=False)
    op.drop_index(op.f('ix_mutations_allele_id'), table_name='mutations')
    op.create_index('allele_id_idx', 'intra_host_variants', ['allele_id'], unique=False)
    # ### end Alembic commands ###
