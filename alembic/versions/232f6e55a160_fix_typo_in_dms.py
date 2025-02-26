"""fix_typo_in_dms

Revision ID: 232f6e55a160
Revises: 7a0327310025
Create Date: 2025-02-25 16:22:58.734701

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '232f6e55a160'
down_revision: Union[str, None] = '7a0327310025'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # ### commands auto generated by Alembic - please adjust! ###
    op.add_column('dms_results', sa.Column('entry_293T', sa.Double(), nullable=False))
    op.drop_column('dms_results', 'entry_239T')
    # ### end Alembic commands ###


def downgrade() -> None:
    # ### commands auto generated by Alembic - please adjust! ###
    op.add_column('dms_results', sa.Column('entry_239T', sa.DOUBLE_PRECISION(precision=53), autoincrement=False, nullable=False))
    op.drop_column('dms_results', 'entry_293T')
    # ### end Alembic commands ###
