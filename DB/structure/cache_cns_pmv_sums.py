from DB.structure.utils import run_sql_file


async def create_all():
    await run_sql_file('sql/cache_cns_pmv_sums/create_matview.sql')
