from sqlalchemy import select, and_

from DB.engine import get_async_session
from DB.models import AnnotationPaper



async def insert_annotation_paper(pmr: AnnotationPaper) -> bool:
    async with get_async_session() as session:
        existing: AnnotationPaper = await session.scalar(
            select(AnnotationPaper)
            .where(
                and_(
                    AnnotationPaper.annotation_id == pmr.annotation_id,
                    AnnotationPaper.paper_id == pmr.annotation_id
                )
            )
        )
        if existing is None:
            session.add(pmr)
            await session.commit()
        return existing