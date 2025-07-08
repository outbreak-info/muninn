from sqlalchemy import select, and_

from DB.engine import get_async_session
from DB.models import Annotation



async def find_or_insert_annotation(pmr: Annotation) -> bool:
    async with get_async_session() as session:
        id_ = await session.scalar(
            select(Annotation.id)
            .where(
                and_(
                    Annotation.amino_acid_substitution_id == pmr.amino_acid_substitution_id,
                    Annotation.effect_id == pmr.effect_id
                )                
            )
        )
        if id_ is None:
            session.add(pmr)
            await session.commit()
            await session.refresh(pmr)
            id_ = pmr.id
    return id_
