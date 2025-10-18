import dask.dataframe as dd
import polars as pl
from sqlalchemy import select, and_

from DB.engine import get_async_write_session, get_asyncpg_connection
from DB.models import Allele
from utils.constants import StandardColumnNames


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


async def copy_insert_alleles(alleles: pl.DataFrame | dd.DataFrame) -> str:
    columns = [
        StandardColumnNames.region,
        StandardColumnNames.position_nt,
        StandardColumnNames.ref_nt,
        StandardColumnNames.alt_nt
    ]
    records = None
    if type(alleles) is dd.DataFrame:
        records = alleles[columns].astype(
            {
                StandardColumnNames.region: str,
                StandardColumnNames.position_nt: int,
                StandardColumnNames.ref_nt: str,
                StandardColumnNames.alt_nt: str
            }
        ).itertuples(index=False, name=None)
    elif type(alleles) is pl.DataFrame:
        records = alleles.select(
            [pl.col(cn) for cn in columns]
        ).iter_rows()
    else:
        raise TypeError

    conn = await get_asyncpg_connection()
    res = await conn.copy_records_to_table(
        Allele.__tablename__,
        records=records,
        columns=columns
    )
    return res
