from utils.run_sql import run_sql_file


async def create_all():
    await run_sql_file('sql/intra_host_translations/create_table_intra_host_translations.sql')
    await run_sql_file('sql/intra_host_translations/create_pk_intra_host_translations.sql')
    await run_sql_file('sql/intra_host_translations/create_fk_sequence_id.sql')
    await run_sql_file('sql/intra_host_translations/create_fk_amino_acid_id.sql')
    await run_sql_file('sql/intra_host_translations/create_ix_amino_acid_id_sequence_id.sql')
