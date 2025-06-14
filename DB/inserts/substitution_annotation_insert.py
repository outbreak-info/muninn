from sqlalchemy import select, and_

from DB.engine import get_async_session
from DB.models import Substitution_Annotation 



async def insert_substitution_annotation(pmr: Substitution_Annotation) -> bool:
    async with get_async_session() as session:
        existing: Substitution_Annotation = await session.scalar(
            select(Substitution_Annotation)
            .where(
                and_(
                    Substitution_Annotation.annotation_id == pmr.annotation_id,
                    Substitution_Annotation.amino_acid_substitution_id == pmr.amino_acid_substitution_id
                )
            )
        )
        if existing is None:
            session.add(pmr)
            await session.commit()
        return existing