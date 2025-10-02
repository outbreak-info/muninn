from datetime import datetime
from math import floor

import polars as pl
from sqlalchemy import select, and_
from sqlalchemy.dialects.postgresql import insert

from DB.engine import get_async_write_session, get_asyncpg_connection
from DB.models import Mutation
from utils.constants import StandardColumnNames, ASYNCPG_MAX_QUERY_ARGS, ConstraintNames


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
        StandardColumnNames.allele_id,
        StandardColumnNames.translation_id
    ]
    conn = await get_asyncpg_connection()

    t0 = datetime.now()
    records = mutations.select(
            [pl.col(cn) for cn in columns]
        ).iter_rows()
    t1 = datetime.now()
    print(f'mutations records took: {t1 - t0}') #rm
    res = await conn.copy_records_to_table(
        Mutation.__tablename__,
        records=records,
        columns=columns
    )
    t2 = datetime.now()
    print(f'copy mutations took {t2 - t1}') #rm
    return res


async def batch_upsert_mutations(mutations: pl.DataFrame):
    update_columns = [StandardColumnNames.translation_id]
    all_columns = update_columns + [
        StandardColumnNames.sample_id,
        StandardColumnNames.allele_id
    ]

    batch_size = floor(ASYNCPG_MAX_QUERY_ARGS / len(all_columns))
    slice_start = 0
    while slice_start < len(mutations):
        mutations_slice = mutations.slice(slice_start, batch_size)
        slice_start += batch_size

        base_insert = (
            insert(Mutation)
            .values(
                mutations_slice
                .select([pl.col(cn) for cn in all_columns])
                .to_dicts()
            )
        )

        async with get_async_write_session() as session:
            await session.execute(
                base_insert.on_conflict_do_update(
                    constraint=ConstraintNames.uq_mutations_sample_allele_pair,
                    set_={k: getattr(base_insert.excluded, k) for k in update_columns}
                )
            )
            await session.commit()
