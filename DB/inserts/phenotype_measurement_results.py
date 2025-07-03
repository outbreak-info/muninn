from sqlalchemy import select, and_

from DB.engine import get_async_write_session
from DB.models import PhenotypeMetricValues


async def insert_pheno_measurement_result(pmr: PhenotypeMetricValues, upsert: bool = False) -> bool:
    updated_existing = False
    async with get_async_write_session() as session:
        existing: PhenotypeMetricValues = await session.scalar(
            select(PhenotypeMetricValues)
            .where(
                and_(
                    PhenotypeMetricValues.amino_acid_id == pmr.amino_acid_id,
                    PhenotypeMetricValues.phenotype_metric_id == pmr.phenotype_metric_id
                )
            )
        )
        if existing is None:
            session.add(pmr)
            await session.commit()
        elif existing.value != pmr.value:
            if upsert:
                updated_existing = True
                existing.value = pmr.value
                await session.commit()
            else:
                raise ValueError('phenotype measurement result value mismatch')
        return updated_existing
