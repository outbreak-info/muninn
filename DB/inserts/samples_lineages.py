from DB.engine import get_async_session
from DB.models import SampleLineage


async def insert_sample_lineage(sl: SampleLineage) -> int:
    async with get_async_session() as session:
        session.add(sl)
        await session.commit()
        await session.refresh(sl)
        id_ = sl.id
    return id_