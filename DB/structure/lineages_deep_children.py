from utils.run_sql import run_sql_file


async def create_all():
    await run_sql_file('sql/lineages_deep_children/create_view_lineages_deep_children.sql')
    await run_sql_file('sql/lineages_deep_children/create_function_check_cyclic_lineage.sql')
    await run_sql_file('sql/lineages_deep_children/create_trigger_check_cyclic_lineage.sql')
    await run_sql_file('sql/lineages_deep_children/create_function_check_cross_system_lineage.sql')
    await run_sql_file('sql/lineages_deep_children/create_trigger_check_cross_system_lineage.sql')