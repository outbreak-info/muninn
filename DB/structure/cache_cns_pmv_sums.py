from DB.engine import get_async_write_session
from DB.structure.utils import run_sql_file
from DB.textutils import text
from utils.constants import TableNames


async def create_all():
    await run_sql_file('sql/cache_cns_pmv_sums/create_table_cache_cns_pmv_sums.sql')
    await refresh_cache()


async def refresh_cache():
    async with get_async_write_session() as session:
        await session.execute(
            text(f'truncate table {TableNames.cache_cns_pmv_sums};')
        )
        await session.commit()
    await run_sql_file('sql/cache_cns_pmv_sums/fill_table.sql')
