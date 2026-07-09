from DB.structure.utils import run_sql_file


async def create_all():
    await run_sql_file('sql/cns_amino_acids_by_sample/create_table_cns_amino_acids_by_sample.sql')
    await run_sql_file('sql/cns_amino_acids_by_sample/create_pk_cns_amino_acids_by_sample.sql')
    await run_sql_file('sql/cns_amino_acids_by_sample/create_fk_sample_id.sql')