from sqlalchemy import select, and_

from DB.engine import get_async_session
from DB.models import Annotation



async def insert_annotation(pmr: Annotation) -> bool:
    async with get_async_session() as session:
        session.add(pmr)
        await session.commit()
        await session.refresh(pmr)
        id_ = pmr.id
    return id_
