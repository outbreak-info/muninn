from sqlalchemy import select, and_

from DB.engine import get_async_session
from DB.models import Effect



async def find_or_insert_effect(pmr: Effect) -> bool:
    async with get_async_session() as session:
        id_ = await session.scalar(
            select(Effect.id)
            .where(
                and_(
                    Effect.detail == pmr.detail,
                )                
            )
        )
        if id_ is None:
            session.add(pmr)
            await session.commit()
            await session.refresh(pmr)
            id_ = pmr.id
    return id_
