from DB.structure.utils import run_sql_file


async def create_all():
    await run_sql_file('sql/papers/create_table_papers.sql')
    await run_sql_file('sql/papers/create_pk_papers.sql')
    await run_sql_file('sql/papers/create_uq_authors_title_year.sql')
