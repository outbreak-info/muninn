import polars as pl
from sqlalchemy import select, and_, insert

from DB.engine import get_async_session
from DB.models import Mutation
from utils.gathering_task_group import GatheringTaskGroup


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


async def batch_insert_mutations(mutations: pl.DataFrame):
    async with get_async_session() as session:
        await session.execute(
            insert(Mutation),
            mutations.to_dicts()
        )
        await session.commit()