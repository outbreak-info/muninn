from sqlalchemy import select, and_

from DB.engine import get_async_write_session
from DB.models import AnnotationPaper


async def find_or_insert_annotation_paper(annotation_paper: AnnotationPaper) -> int:
    async with get_async_write_session() as session:
        id_: int = await session.scalar(
            select(AnnotationPaper.id)
            .where(
                and_(
                    AnnotationPaper.annotation_id == annotation_paper.annotation_id,
                    AnnotationPaper.paper_id == annotation_paper.paper_id
                )
            )
        )
        if id_ is None:
            session.add(annotation_paper)
            await session.commit()
            await session.refresh(annotation_paper)
            id_ = annotation_paper.id
        return id_