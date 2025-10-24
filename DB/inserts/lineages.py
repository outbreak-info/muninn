import polars as pl
from sqlalchemy import select, and_

from DB.engine import get_async_write_session, get_asyncpg_connection
from DB.models import Lineage
from utils.constants import StandardColumnNames


async def find_or_insert_lineage(lin: Lineage) -> int:
    async with get_async_write_session() as session:
        id_ = await session.scalar(
            select(Lineage.id)
            .where(
                and_(
                    Lineage.lineage_name == lin.lineage_name,
                    Lineage.lineage_system_id == lin.lineage_system_id
                )
            )
        )
        if id_ is None:
            session.add(lin)
            await session.commit()
            await session.refresh(lin)
            id_ = lin.id
    return id_


async def copy_insert_lineages(lineages: pl.DataFrame):
    columns = [
        StandardColumnNames.lineage_system_id,
        StandardColumnNames.lineage_name
    ]
    conn = await get_asyncpg_connection()
    res = await conn.copy_records_to_table(
        Lineage.__tablename__,
        records=lineages.select(
            [pl.col(cn) for cn in columns]
        ).iter_rows(),
        columns=columns
    )
    return res
