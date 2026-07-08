from DB.structure.utils import run_sql_file


async def create_all():
    await run_sql_file('sql/cns_samples_by_amino_acid/create_table_cns_samples_by_amino_acid.sql')
    await run_sql_file('sql/cns_samples_by_amino_acid/create_pk_cns_samples_by_amino_acid.sql')
    await run_sql_file('sql/cns_samples_by_amino_acid/create_fk_amino_acid_id.sql')