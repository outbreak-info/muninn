from DB.structure.utils import run_sql_file


async def create_all():
    await create_table()
    await create_pk()

async def create_table():
    await run_sql_file('sql/sequences/create_sequences_table.sql')

async def create_pk():
    await run_sql_file('sql/sequences/create_pk_sequences.sql')
