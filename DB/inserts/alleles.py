import logging
from typing import Any

import polars as pl
from sqlalchemy import select, and_, text, insert

from DB.engine import get_async_write_session, get_asyncpg_connection
from DB.models import Allele
from DB.queries.alleles import get_all_alleles_as_pl_df
from utils.constants import StandardColumnNames
from utils.gathering_task_group import GatheringTaskGroup


async def find_or_insert_allele(a: Allele) -> int:
    async with get_async_write_session() as session:
        id_: int = await session.scalar(
            select(Allele.id)
            .where(
                and_(
                    Allele.region == a.region,
                    Allele.position_nt == a.position_nt,
                    Allele.alt_nt == a.alt_nt
                )
            )
        )

        if id_ is None:
            session.add(a)
            await session.commit()
            await session.refresh(a)
            id_ = a.id
    return id_


async def copy_insert_alleles(alleles: pl.DataFrame) -> str:
    columns = [
        StandardColumnNames.region,
        StandardColumnNames.position_nt,
        StandardColumnNames.ref_nt,
        StandardColumnNames.alt_nt
    ]
    conn = await get_asyncpg_connection()
    res = await conn.copy_records_to_table(
        Allele.__tablename__,
        records=alleles.select(
            [pl.col(cn) for cn in columns]
        ).iter_rows(),
        columns=columns
    )
    return res
