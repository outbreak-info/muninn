from utils.run_sql import run_sql_file


async def create_all():
    await run_sql_file('sql/alleles/create_alleles_table.sql')
    await run_sql_file('sql/alleles/create_pk_alleles.sql')
    await run_sql_file('sql/alleles/create_uq_nt_values.sql')
    await run_sql_file('sql/alleles/create_check_alt_nt_not_empty.sql')
    await run_sql_file('sql/alleles/create_check_ref_nt_not_empty.sql')
