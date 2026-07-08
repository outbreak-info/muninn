from utils.run_sql import run_sql_file


async def create_all():
    await run_sql_file('sql/lineage_systems/create_table_lineage_systems.sql')
    await run_sql_file('sql/lineage_systems/create_pk_lineage_systems.sql')
    await run_sql_file('sql/lineage_systems/create_uq_name.sql')
