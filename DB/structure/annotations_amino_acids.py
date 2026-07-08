from DB.structure.utils import run_sql_file


async def create_all():
    await run_sql_file('sql/annotations_amino_acids/create_table_annotations_amino_acids.sql')
    await run_sql_file('sql/annotations_amino_acids/create_pk_annotations_amino_acids.sql')
    await run_sql_file('sql/annotations_amino_acids/create_fk_amino_acid_id.sql')
    await run_sql_file('sql/annotations_amino_acids/create_fk_annotation_id.sql')
    await run_sql_file('sql/annotations_amino_acids/create_uq_pair.sql')
