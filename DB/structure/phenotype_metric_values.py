from utils.run_sql import run_sql_file


async def create_all():
    await run_sql_file('sql/phenotype_metric_values/create_table_phenotype_metric_values.sql')
    await run_sql_file('sql/phenotype_metric_values/create_pk_phenotype_metric_values.sql')
    await run_sql_file('sql/phenotype_metric_values/create_fk_phenotype_metric_id.sql')
    await run_sql_file('sql/phenotype_metric_values/create_fk_amino_acid_id.sql')
    await run_sql_file('sql/phenotype_metric_values/create_uq_metric_and_amino_acid.sql')
