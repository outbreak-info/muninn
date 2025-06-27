from sqlalchemy import select

from DB.engine import get_async_write_session
from DB.models import PhenotypeMetric


async def find_or_insert_metric(pm: PhenotypeMetric) -> int:
    async with get_async_write_session() as session:
        id_ = await session.scalar(
            select(PhenotypeMetric.id)
            .where(
                # they are uq by name, so this is enough
                PhenotypeMetric.name == pm.name
            )
        )
        if id_ is None:
            session.add(pm)
            await session.commit()
            await session.refresh(pm)
            id_ = pm.id
    return id_