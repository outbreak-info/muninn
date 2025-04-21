from sqlalchemy import select, and_

from DB.engine import get_async_session
from DB.models import PhenotypeMeasurementResult


async def insert_pheno_measurement_result(pmr: PhenotypeMeasurementResult, upsert: bool = False) -> bool:
    updated_existing = False
    async with get_async_session() as session:
        existing: PhenotypeMeasurementResult = await session.scalar(
            select(PhenotypeMeasurementResult)
            .where(
                and_(
                    PhenotypeMeasurementResult.amino_acid_substitution_id == pmr.amino_acid_substitution_id,
                    PhenotypeMeasurementResult.phenotype_metric_id == pmr.phenotype_metric_id
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
