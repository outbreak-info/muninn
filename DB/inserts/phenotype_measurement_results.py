from DB.engine import get_async_write_session
from DB.textutils import text
from utils.constants import TableNames, ColumnNames


async def upsert_pheno_measurement_result(
    amino_acid_id: int,
    phenotype_metric_id: int,
    value: float
) -> bool:
    async with get_async_write_session() as session:
        # info on the magic at the end of this query, see: https://stackoverflow.com/q/39058213
        updated_existing = await session.scalar(
            text(
                f'''
                insert into {TableNames.phenotype_metric_values} 
                ({ColumnNames.phenotype_metric_id}, {ColumnNames.amino_acid_id}, {ColumnNames.value})
                values (:metric_id, :aa_id, :value)
                on conflict ({ColumnNames.phenotype_metric_id}, {ColumnNames.amino_acid_id}) 
                do update set {ColumnNames.value} = excluded.{ColumnNames.value}
                returning xmax <> 0 as updated;
                '''
            ),
            {
                'metric_id': phenotype_metric_id,
                'aa_id': amino_acid_id,
                'value': value
            }
        )
        await session.commit()

    return updated_existing
