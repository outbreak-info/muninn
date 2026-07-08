from utils.run_sql import run_sql_file


async def create_all():
    await run_sql_file('sql/lineages_immediate_children/create_table_lineages_immediate_children.sql')
    await run_sql_file('sql/lineages_immediate_children/create_pk_lineages_immediate_children.sql')
    await run_sql_file('sql/lineages_immediate_children/create_fk_parent_id.sql')
    await run_sql_file('sql/lineages_immediate_children/create_fk_child.sql')
    await run_sql_file('sql/lineages_immediate_children/create_check_no_self_parenthood.sql')