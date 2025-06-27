import logging
from typing import Any

import polars as pl
from sqlalchemy import select, and_, text, insert

from DB.engine import get_async_session, get_asyncpg_connection
from DB.models import Allele
from DB.queries.alleles import get_all_alleles_as_pl_df
from utils.constants import StandardColumnNames
from utils.gathering_task_group import GatheringTaskGroup


async def find_or_insert_allele(a: Allele) -> int:
    async with get_async_session() as session:
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


async def batch_insert_alleles(
    alleles: pl.DataFrame,
    position_nt_name: str = 'position_nt',
    ref_nt_name: str = 'ref_nt',
    alt_nt_name: str = 'alt_nt',
    region_name: str = 'region'
) -> pl.DataFrame:
    """
    Insert all alleles described in a dataframe
    :param alleles: polars df with cols: region, position_nt, ref_nt, alt_nt
    :param region_name:
    :param alt_nt_name:
    :param ref_nt_name:
    :param position_nt_name:
    :return: Input df with 'id' col added, giving the database id of each allele
    """

    async with get_async_session() as session:
        ids = await session.scalars(
            insert(Allele).returning(Allele.id),
            alleles.to_dicts()
        )
        await session.commit()

    alleles = alleles.with_columns(
        pl.Series('allele_id', ids)
    )

    return alleles


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


async def bulk_insert_new_alleles_skip_existing(alleles: pl.DataFrame) -> pl.DataFrame:
    # 1. get all the alleles from the db and do some filtering
    existing_alleles = await get_all_alleles_as_pl_df()

    alleles = alleles.join(
        existing_alleles,
        on=[
            StandardColumnNames.region,
            StandardColumnNames.position_nt,
            StandardColumnNames.alt_nt
        ],
        how='left'
    )

    # check if the ref nts match
    count_mismatched_refs = alleles.filter(pl.col(StandardColumnNames.ref_nt) != pl.col('ref_nt_right'))
    if len(count_mismatched_refs) > 0:
        print(f'Found {count_mismatched_refs} alleles with mismatched ref_nt.')

    new_alleles = alleles.filter(
        pl.col(StandardColumnNames.allele_id).is_null()
    ).select(
        StandardColumnNames.region,
        StandardColumnNames.position_nt,
        StandardColumnNames.ref_nt,
        StandardColumnNames.alt_nt
    )

    # 2. do a bulk insert of all new alleles
    conn = await get_asyncpg_connection()
    res = await conn.copy_records_to_table(
        'alleles',
        records=new_alleles.iter_rows(),
        columns=[
            StandardColumnNames.region,
            StandardColumnNames.position_nt,
            StandardColumnNames.ref_nt,
            StandardColumnNames.alt_nt
        ]
    )

    # 3. get IDs back and return a df with ids added

    existing_alleles = await get_all_alleles_as_pl_df()
    new_alleles = new_alleles.join(
        existing_alleles,
        on=[
            StandardColumnNames.region,
            StandardColumnNames.position_nt,
            StandardColumnNames.alt_nt
        ],
        how='left'
    )
    return new_alleles
