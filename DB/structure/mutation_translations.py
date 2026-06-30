from sqlalchemy.sql.expression import text

from DB.engine import get_async_write_session
from utils.constants import StandardColumnNames, TableNames, ConstraintNames


async def drop_existing_mutation_translations():
    async with get_async_write_session() as session:
        test_id = (await session.execute(
            text(
                f'select {StandardColumnNames.amino_acid_id} from {TableNames.mutation_translations} limit 1;'
            )
        )).scalar()
        if test_id is not None:
            raise RuntimeError('mutation translations table is not empty, so I will not drop it.')

        await session.execute(
            text(
                f'drop table if exists {TableNames.mutation_translations}'
            )
        )
        await session.commit()


async def create_bitmap_mutation_translations_table():
    async with get_async_write_session() as session:
        await session.execute(
            text(
                f'create table {TableNames.mutation_translations} (\n'
                f'{StandardColumnNames.amino_acid_id} bigint,\n'
                f'{StandardColumnNames.sequences_present} roaringbitmap storage extended not null\n'
                f');'
            )
        )
        await session.execute(text(
            f'alter table {TableNames.mutation_translations}\n'
            f'add constraint {ConstraintNames.pk_mutation_translations}\n'
            f'primary key ({StandardColumnNames.amino_acid_id});'
        ))

        await session.execute(text(
            f'alter table {TableNames.mutation_translations}\n'
            f'add constraint {ConstraintNames.fk_mutation_translations_amino_acid_id_amino_acids}\n'
            f'foreign key ({StandardColumnNames.amino_acid_id}) references {TableNames.amino_acids} (id);'
        ))
        await session.commit()
