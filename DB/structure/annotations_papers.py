from DB.structure.utils import run_sql_file


async def create_all():
    await run_sql_file('sql/annotations_papers/create_table_annotations_papers.sql')
    await run_sql_file('sql/annotations_papers/create_pk_annotations_papers.sql')
    await run_sql_file('sql/annotations_papers/create_fk_paper_id.sql')
    await run_sql_file('sql/annotations_papers/create_fk_annotation_id.sql')
    await run_sql_file('sql/annotations_papers/create_uq_annotation_paper_pair.sql')
