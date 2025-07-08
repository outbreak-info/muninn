from sqlalchemy import select, and_

from DB.engine import get_async_write_session
from DB.models import Effect


async def find_or_insert_effect(e: Effect) -> bool:
    async with get_async_write_session() as session:
        id_ = await session.scalar(
            select(Effect.id)
            .where(
                and_(
                    Effect.detail == e.detail,
                )                
            )
        )
        if id_ is None:
            session.add(e)
            await session.commit()
            await session.refresh(e)
            id_ = e.id
    return id_
