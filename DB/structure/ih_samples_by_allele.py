from sqlalchemy.sql.expression import text

from DB.engine import get_async_write_session
from DB.structure.utils import run_sql_file
from utils.constants import TableNames, ColumnNames


async def create_all():
    await run_sql_file('sql/ih_samples_by_allele/create_table_ih_samples_by_allele.sql')
    await run_sql_file('sql/ih_samples_by_allele/create_pk_ih_samples_by_allele.sql')
    await run_sql_file('sql/ih_samples_by_allele/create_fk_ih_samples_by_allele_allele_id_alleles.sql')
    await run_sql_file('sql/ih_samples_by_allele/create_function_check_range_overlap.sql')
    await run_sql_file('sql/ih_samples_by_allele/create_trigger_check_range_overlap.sql')


async def drop_trigger_check_range_overlap():
    await run_sql_file('sql/ih_samples_by_allele/drop_trigger_check_range_overlap.sql')


async def restore_trigger_check_range_overlap():
    await run_sql_file('sql/ih_samples_by_allele/create_trigger_check_range_overlap.sql')
    async with get_async_write_session() as session:
        await session.execute(
            text(f'update {TableNames.ih_samples_by_allele} set {ColumnNames.allele_id} = {ColumnNames.allele_id};')
        )
        await session.commit()