from sqlalchemy import select, and_

from DB.engine import get_async_session
from DB.models import Paper 



async def find_or_insert_paper(pmr: Paper) -> bool:
    async with get_async_session() as session:
        id_ = await session.scalar(
            select(Paper.id)
            .where(
                and_(
                    Paper.author == pmr.author,
                    Paper.publication_year == pmr.publication_year
                )                
            )
        )
        if id_ is None:
            session.add(pmr)
            await session.commit()
            await session.refresh(pmr)
            id_ = pmr.id
    return id_
