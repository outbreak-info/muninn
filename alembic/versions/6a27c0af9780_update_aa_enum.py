"""update_aa_enum

Revision ID: 6a27c0af9780
Revises: c9f647521b3c
Create Date: 2025-02-13 11:43:20.643643

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa

from DB.enums import AminoAcid
from DB.models import Allele

# revision identifiers, used by Alembic.
revision: str = '6a27c0af9780'
down_revision: Union[str, None] = 'c9f647521b3c'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


new_constraints = [
    AminoAcid.get_check_constraint('ref_aa'),
    AminoAcid.get_check_constraint('alt_aa')
]

def upgrade() -> None:
    with op.batch_alter_table(Allele.__tablename__) as batch_op:
        for constraint in new_constraints:
            # drop old constraints, they'll have the same names as the new ones
            batch_op.drop_constraint(f'ck_alleles_`{constraint.name}`')
            # create new constraints from existing enums
            batch_op.create_check_constraint(constraint.name, constraint.sqltext)


def downgrade() -> None:
    raise NotImplementedError
