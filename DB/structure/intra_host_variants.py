from DB.structure.utils import run_sql_file


async def create_all():
    await run_sql_file('sql/intra_host_variants/create_table_intra_host_variants.sql')
    await run_sql_file('sql/intra_host_variants/create_pk_intra_host_variants.sql')
    await run_sql_file('sql/intra_host_variants/create_fk_sample_id.sql')
    await run_sql_file('sql/intra_host_variants/create_fk_allele_id.sql')
    await run_sql_file('sql/intra_host_variants/create_ix_allele_id_sample_id.sql')
