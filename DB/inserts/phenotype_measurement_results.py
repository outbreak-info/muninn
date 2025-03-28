from sqlalchemy import select, and_

from DB.engine import get_async_session
from DB.models import PhenotypeMeasurementResult


async def insert_pheno_measurement_result(pmr: PhenotypeMeasurementResult) -> None:
    async with get_async_session() as session:
        v = await session.scalar(
            select(PhenotypeMeasurementResult.value)
            .where(
                and_(
                    PhenotypeMeasurementResult.amino_acid_substitution_id == pmr.amino_acid_substitution_id,
                    PhenotypeMeasurementResult.phenotype_metric_id == pmr.phenotype_metric_id
                )
            )
        )
        if v is None:
            session.add(pmr)
            await session.commit()
        elif v != pmr.value:
            raise ValueError('phenotype measurement result value mismatch')
