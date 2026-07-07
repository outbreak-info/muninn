from DB.structure.utils import run_sql_file


async def create_all():
    await run_sql_file('sql/consensus_sequences_by_amino_acid/create_table_consensus_sequences_by_amino_acid.sql')
    await run_sql_file('sql/consensus_sequences_by_amino_acid/create_pk_consensus_sequences_by_amino_acid.sql')
    await run_sql_file('sql/consensus_sequences_by_amino_acid/create_fk_amino_acid_id.sql')