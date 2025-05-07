from typing import List

from sqlalchemy import select

from DB.engine import get_async_session
from DB.models import PhenotypeMetric
from api.models import PhenotypeMetricInfo


async def get_all_pheno_metrics() -> List[PhenotypeMetricInfo]:
    async with get_async_session() as session:
        res = await session.scalars(
            select(PhenotypeMetric)
        )
        out_data = [PhenotypeMetricInfo.from_db_object(pm) for pm in res]
    return out_data