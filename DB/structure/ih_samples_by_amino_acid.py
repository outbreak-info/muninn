from DB.structure.utils import run_sql_file


async def create_all():
    await run_sql_file('sql/ih_samples_by_amino_acid/create_table_ih_samples_by_amino_acid.sql')
    await run_sql_file('sql/ih_samples_by_amino_acid/create_pk_ih_samples_by_amino_acid.sql')
    await run_sql_file('sql/ih_samples_by_amino_acid/create_fk_amino_acid_id.sql')
    await run_sql_file('sql/ih_samples_by_amino_acid/create_function_check_range_overlap.sql')
    await run_sql_file('sql/ih_samples_by_amino_acid/create_trigger_check_range_overlap.sql')