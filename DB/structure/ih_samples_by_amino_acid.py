from sqlalchemy.sql.expression import text

from DB.engine import get_async_write_session
from DB.structure.utils import run_sql_file
from utils.constants import TableNames, ColumnNames


async def create_all():
    await run_sql_file('sql/ih_samples_by_amino_acid/create_table_ih_samples_by_amino_acid.sql')
    await run_sql_file('sql/ih_samples_by_amino_acid/create_pk_ih_samples_by_amino_acid.sql')
    await run_sql_file('sql/ih_samples_by_amino_acid/create_fk_ih_samples_by_amino_acid_amino_acid_id_amino_acids.sql')
    await run_sql_file('sql/ih_samples_by_amino_acid/create_function_check_range_overlap.sql')
    await run_sql_file('sql/ih_samples_by_amino_acid/create_trigger_check_range_overlap.sql')


async def drop_trigger_check_range_overlap():
    await run_sql_file('sql/ih_samples_by_amino_acid/drop_trigger_check_range_overlap.sql')


async def restore_trigger_check_range_overlap():
    await run_sql_file('sql/ih_samples_by_amino_acid/create_trigger_check_range_overlap.sql')
    async with get_async_write_session() as session:
        await session.execute(
            text(
                f'update {TableNames.ih_samples_by_amino_acid} set {ColumnNames.amino_acid_id} = {ColumnNames.amino_acid_id};'
            )
        )
        await session.commit()
