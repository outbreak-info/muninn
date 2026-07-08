from DB.structure.utils import run_sql_file


async def create_all():
    await run_sql_file('sql/effects/create_table_effects.sql')
    await run_sql_file('sql/effects/create_pk_effects.sql')
    await run_sql_file('sql/effects/create_uq_detail.sql')
