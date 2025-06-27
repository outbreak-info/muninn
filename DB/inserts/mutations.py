import polars as pl
from sqlalchemy import select, and_

from DB.engine import get_async_write_session, get_asyncpg_connection
from DB.models import Mutation
from utils.constants import StandardColumnNames


async def find_or_insert_mutation(m: Mutation) -> int:
    async with get_async_write_session() as session:
        id_ = await session.scalar(
            select(Mutation.id)
            .where(
                and_(
                    Mutation.allele_id == m.allele_id,
                    Mutation.sample_id == m.sample_id
                )
            )
        )
        if id_ is None:
            session.add(m)
            await session.commit()
            await session.refresh(m)
            id_ = m.id
    return id_


async def copy_insert_mutations(mutations: pl.DataFrame) -> str:
    columns = [
        StandardColumnNames.sample_id,
        StandardColumnNames.allele_id
    ]
    conn = await get_asyncpg_connection()
    res = await conn.copy_records_to_table(
        Mutation.__tablename__,
        records=mutations.select(
            [pl.col(cn) for cn in columns]
        ).iter_rows(),
        columns=columns
    )
    return res
