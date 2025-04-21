from DB.engine import get_async_session
from DB.models import Mutation
from sqlalchemy import select, and_


async def find_or_insert_mutation(m: Mutation) -> int:
    async with get_async_session() as session:
        id_ = await session.scalar(
            select(Mutation.id)
            .where(and_(
                Mutation.allele_id == m.allele_id,
                Mutation.sample_id == m.sample_id
            ))
        )
        if id_ is None:
            session.add(m)
            await session.commit()
            await session.refresh(m)
            id_ = m.id
    return id_