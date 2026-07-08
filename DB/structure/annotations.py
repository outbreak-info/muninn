from DB.structure.utils import run_sql_file


async def create_all():
    await run_sql_file('sql/annotations/create_table_annotations.sql')
    await run_sql_file('sql/annotations/create_pk_annotations.sql')
    await run_sql_file('sql/annotations/create_fk_effect_id.sql')
