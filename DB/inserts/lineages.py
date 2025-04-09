from sqlalchemy import select, and_

from DB.engine import get_async_session
from DB.models import Lineage


async def find_or_insert_lineage(lin: Lineage) -> int:
    async with get_async_session() as session:
        id_ = await session.scalar(
            select(Lineage.id)
            .where(
                and_(
                    Lineage.lineage_name == lin.lineage_name,
                    Lineage.lineage_system_id == lin.lineage_system_id
                )
            )
        )
        if id_ is None:
            session.add(lin)
            await session.commit()
            await session.refresh(lin)
            id_ = lin.id
    return id_


