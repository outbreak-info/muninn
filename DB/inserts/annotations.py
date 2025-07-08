from sqlalchemy import select, and_

from DB.engine import get_async_write_session
from DB.models import Annotation


async def find_or_insert_annotation(a: Annotation) -> int:
    async with get_async_write_session() as session:
        id_ = await session.scalar(
            select(Annotation.id)
            .where(
                and_(
                    Annotation.amino_acid_id == a.amino_acid_id,
                    Annotation.effect_id == a.effect_id
                )                
            )
        )
        if id_ is None:
            session.add(a)
            await session.commit()
            await session.refresh(a)
            id_ = a.id
    return id_
