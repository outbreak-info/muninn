from DB.structure.utils import run_sql_file


async def create_all():
    await run_sql_file('sql/phenotype_metrics/create_table_phenotype_metrics.sql')
    await run_sql_file('sql/phenotype_metrics/create_pk_phenotype_metrics.sql')
    await run_sql_file('sql/phenotype_metrics/create_uq_name.sql')
    await run_sql_file('sql/phenotype_metrics/create_check_name_not_empty.sql')
    await run_sql_file('sql/phenotype_metrics/create_check_assay_type_not_empty.sql')
