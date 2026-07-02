from DB.structure.runner import run_sql_file


async def create_all():
    await run_sql_file('sql/amino_acids/create_amino_acids_table.sql')
    await run_sql_file('sql/amino_acids/create_pk_amino_acids.sql')
    await run_sql_file('sql/amino_acids/create_uq_aa_values.sql')
    await run_sql_file('sql/amino_acids/create_check_gff_feature_not_empty.sql')
    await run_sql_file('sql/amino_acids/create_check_alt_aa_not_empty.sql')
    await run_sql_file('sql/amino_acids/create_check_ref_aa_not_empty.sql')
    await run_sql_file('sql/amino_acids/create_check_ref_codon_not_empty.sql')
    await run_sql_file('sql/amino_acids/create_check_alt_codon_not_empty.sql')