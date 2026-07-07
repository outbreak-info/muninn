from DB.structure.utils import run_sql_file


async def create_all():
    await run_sql_file('sql/consensus_sequences_by_allele/create_table_consensus_sequences_by_allele.sql')
    await run_sql_file('sql/consensus_sequences_by_allele/create_pk_consensus_sequences_by_allele.sql')
    await run_sql_file('sql/consensus_sequences_by_allele/create_fk_allele_id.sql')