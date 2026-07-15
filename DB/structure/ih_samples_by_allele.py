from DB.structure.utils import run_sql_file


async def create_all():
    await run_sql_file('sql/ih_samples_by_allele/create_table_ih_samples_by_allele.sql')
    await run_sql_file('sql/ih_samples_by_allele/create_pk_ih_samples_by_allele.sql')
    await run_sql_file('sql/ih_samples_by_allele/create_fk_ih_samples_by_allele_allele_id_alleles.sql')
    await run_sql_file('sql/ih_samples_by_allele/create_function_check_range_overlap.sql')
    await run_sql_file('sql/ih_samples_by_allele/create_trigger_check_range_overlap.sql')
