from DB.structure.utils import run_sql_file


async def create_all():
    await run_sql_file('sql/samples_lineages/create_table_samples_lineages.sql')
    await run_sql_file('sql/samples_lineages/create_pk_samples_lineages.sql')
    await run_sql_file('sql/samples_lineages/create_fk_sample_id.sql')
    await run_sql_file('sql/samples_lineages/create_fk_lineage_id.sql')
    await run_sql_file('sql/samples_lineages/create_uq_sample_lineage_consensus.sql')
    await run_sql_file('sql/samples_lineages/create_check_abundance_xor_consensus.sql')
    await run_sql_file('sql/samples_lineages/create_ix_lineage_id.sql')
