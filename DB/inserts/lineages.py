import polars as pl
from sqlalchemy import select, and_

from DB.engine import get_async_write_session, get_asyncpg_connection, get_uri_for_polars
from DB.models import Lineage
from utils.constants import StandardColumnNames, TableNames


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


async def get_all_lineages_by_lineage_system_as_pl_df(lineage_system_name: str) -> pl.DataFrame:
    return pl.read_database_uri(
        query=f'''
        select 
            l.id as {StandardColumnNames.lineage_id},
            l.{StandardColumnNames.lineage_name}
        from {TableNames.lineages} l
        inner join {TableNames.lineage_systems} ls on ls.id = l.{StandardColumnNames.lineage_system_id}
        where ls.{StandardColumnNames.lineage_system_name} = '{lineage_system_name}'
        ''',
        uri=get_uri_for_polars()
    )
