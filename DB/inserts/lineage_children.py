from math import floor

import polars as pl
from sqlalchemy import and_, or_
from sqlalchemy.sql.expression import delete

from DB.engine import get_async_write_session, get_asyncpg_connection
from DB.models import LineageImmediateChild
from utils.constants import StandardColumnNames, ASYNCPG_MAX_QUERY_ARGS


async def copy_insert_lineage_children(relationships: pl.DataFrame):
    columns = [
        StandardColumnNames.parent_id,
        StandardColumnNames.child_id
    ]
    conn = await get_asyncpg_connection()
    res = await conn.copy_records_to_table(
        LineageImmediateChild.__tablename__,
        records=relationships.select(
            [pl.col(cn) for cn in columns]
        ).iter_rows(),
        columns=columns
    )
    return res


async def batch_delete_lineage_children(dropped_relationships: pl.DataFrame):
    delete_data_columns = [
        StandardColumnNames.parent_id,
        StandardColumnNames.child_id
    ]

    batch_size = floor(ASYNCPG_MAX_QUERY_ARGS / len(delete_data_columns))
    for batch in dropped_relationships.iter_slices(batch_size):
        data = batch.to_dicts()

        where_clause = None
        for d in data:
            where_clause = or_(
                where_clause,
                and_(
                    LineageImmediateChild.parent_id == d[StandardColumnNames.parent_id],
                    LineageImmediateChild.child_id == d[StandardColumnNames.child_id]
                )
            )
        delete_statement = (
            delete(LineageImmediateChild)
            .where(
                where_clause
            )
        )
        async with get_async_write_session() as session:
            await session.execute(delete_statement)
            await session.commit()
