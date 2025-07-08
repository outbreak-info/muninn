from sqlalchemy import select, and_

from DB.engine import get_async_write_session
from DB.models import AnnotationPaper


async def find_or_insert_annotation_paper(annotation_paper: AnnotationPaper) -> bool:
    async with get_async_write_session() as session:
        existing: AnnotationPaper = await session.scalar(
            select(AnnotationPaper)
            .where(
                and_(
                    AnnotationPaper.annotation_id == annotation_paper.annotation_id,
                    AnnotationPaper.paper_id == annotation_paper.paper_id
                )
            )
        )
        if existing is None:
            session.add(annotation_paper)
            await session.commit()
        return existing is not None