from DB.structure.utils import run_sql_file


async def create_all():
    await run_sql_file('sql/lineages/create_table_lineages.sql')
    await run_sql_file('sql/lineages/create_pk_lineages.sql')
    await run_sql_file('sql/lineages/create_fk_lineage_system_id.sql')
    await run_sql_file('sql/lineages/create_uq_name_within_system.sql')
