from sqlalchemy import select, and_

from DB.engine import get_async_session
from DB.models import Annotation_Paper 



async def insert_annotation_paper(pmr: Annotation_Paper) -> bool:
    async with get_async_session() as session:
        existing: Annotation_Paper = await session.scalar(
            select(Annotation_Paper)
            .where(
                and_(
                    Annotation_Paper.annotation_id == pmr.annotation_id,
                    Annotation_Paper.paper_id == pmr.paper_id
                )
            )
        )
        if existing is None:
            session.add(pmr)
            await session.commit()
        return existing