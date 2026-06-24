from sqlalchemy.sql.expression import text

from DB.engine import get_async_write_session
from utils.constants import StandardColumnNames, TableNames, ConstraintNames


async def drop_existing_mutations():
    async with get_async_write_session() as session:
        test_id = (await session.execute(
            text(
                f'select {StandardColumnNames.allele_id} from mutations limit 1;'
            )
        )).scalar()
        if test_id is not None:
            raise RuntimeError('Mutations table is not empty, so I will not drop it.')

        await session.execute(
            text(
                f'drop table if exists {TableNames.mutations}'
            )
        )
        await session.commit()


async def create_bitmap_mutations_table():
    async with get_async_write_session() as session:
        await session.execute(
            text(
                f'create table {TableNames.mutations} (\n'
                f'{StandardColumnNames.allele_id} integer,\n'
                f'{StandardColumnNames.sequences_present} roaringbitmap storage extended not null\n'
                f');'
            )
        )
        await session.execute(text(
            f'alter table mutations add constraint {ConstraintNames.pk_mutations}\n'
            f'primary key ({StandardColumnNames.allele_id});'
        ))

        await session.execute(text(
            f'alter table mutations add constraint {ConstraintNames.fk_mutations_allele_id_alleles}\n'
            f'foreign key (allele_id) references alleles (id);'
        ))
        await session.commit()



