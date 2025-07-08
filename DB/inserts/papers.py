from sqlalchemy import select, and_

from DB.engine import get_async_write_session
from DB.models import Paper


async def find_or_insert_paper(p: Paper) -> int:
    async with get_async_write_session() as session:
        id_ = await session.scalar(
            select(Paper.id)
            .where(
                and_(
                    Paper.authors == p.authors,
                    Paper.title == p.title,
                    Paper.publication_year == p.publication_year
                )                
            )
        )
        if id_ is None:
            session.add(p)
            await session.commit()
            await session.refresh(p)
            id_ = p.id
    return id_
