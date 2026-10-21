from DB.engine import get_async_write_session
from DB.textutils import text
from utils.constants import TableNames, ColumnNames


async def find_or_insert_metric(phenotype_metric_name: str, phenotype_metric_assay_type: str) -> int:
    async with get_async_write_session() as session:
        id_ = await session.scalar(
            text(
                f'''
                select id from {TableNames.phenotype_metrics}
                where {ColumnNames.phenotype_metric_name} = :name;
                '''
            ),
            {'name': phenotype_metric_name}
        )
        if id_ is None:
            id_ = await session.scalar(
                text(
                    f'''
                    insert into {TableNames.phenotype_metrics} 
                    ({ColumnNames.phenotype_metric_name}, {ColumnNames.phenotype_metric_assay_type})
                    values 
                    (:name, :assay_type)
                    returning id;
                    '''
                ),
                {
                    'name': phenotype_metric_name,
                    'assay_type': phenotype_metric_assay_type
                }
            )
            await session.commit()
    return id_
