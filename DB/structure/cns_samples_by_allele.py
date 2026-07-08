from DB.structure.utils import run_sql_file


async def create_all():
    await run_sql_file('sql/cns_samples_by_allele/create_table_cns_samples_by_allele.sql')
    await run_sql_file('sql/cns_samples_by_allele/create_pk_cns_samples_by_allele.sql')
    await run_sql_file('sql/cns_samples_by_allele/create_fk_allele_id.sql')