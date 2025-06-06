from math import floor

import polars as pl
from sqlalchemy import select, and_
from sqlalchemy.dialects.postgresql import insert

from DB.engine import get_async_session, get_asyncpg_connection
from DB.models import IntraHostVariant
from utils.constants import StandardColumnNames, ConstraintNames


async def find_or_insert_variant(variant: IntraHostVariant, upsert: bool = True) -> (int, bool):
    """
    :param variant:
    :return: int: id of variant, new or existing
    bool: true if this variant already existed
    """
    preexisting = True
    async with get_async_session() as session:
        existing = await session.scalar(
            select(IntraHostVariant)
            .where(
                and_(
                    IntraHostVariant.allele_id == variant.allele_id,
                    IntraHostVariant.sample_id == variant.sample_id
                )
            )
        )
        if existing is None:
            preexisting = False
            session.add(variant)
            await session.commit()
            await session.refresh(variant)
            id_ = variant.id
        else:
            id_ = existing.id
            if upsert:
                existing.copy_from(variant)
                await session.commit()

    return id_, preexisting


async def copy_insert_variants(variants: pl.DataFrame):
    columns = [
        StandardColumnNames.ref_dp,
        StandardColumnNames.alt_dp,
        StandardColumnNames.alt_freq,
        StandardColumnNames.ref_rv,
        StandardColumnNames.alt_rv,
        StandardColumnNames.ref_qual,
        StandardColumnNames.alt_qual,
        StandardColumnNames.total_dp,
        StandardColumnNames.pval,
        StandardColumnNames.pass_qc,
    ]
    conn = await get_asyncpg_connection()
    res = await conn.copy_records_to_table(
        IntraHostVariant.__tablename__,
        records=variants.select(
            [pl.col(cn) for cn in columns]
        ).iter_rows(),
        columns=columns
    )
    return res


async def batch_upsert_variants(variants: pl.DataFrame):
    max_query_args = 32767

    update_columns = [
        StandardColumnNames.ref_dp,
        StandardColumnNames.alt_dp,
        StandardColumnNames.alt_freq,
        StandardColumnNames.ref_rv,
        StandardColumnNames.alt_rv,
        StandardColumnNames.ref_qual,
        StandardColumnNames.alt_qual,
        StandardColumnNames.total_dp,
        StandardColumnNames.pval,
        StandardColumnNames.pass_qc,
    ]
    all_columns = update_columns + [
        StandardColumnNames.sample_id,
        StandardColumnNames.allele_id
    ]

    batch_size = floor(max_query_args / len(all_columns))
    slice_start = 0
    while slice_start < len(variants):
        variants_slice = variants.slice(slice_start, batch_size)
        slice_start += batch_size

        base_insert = (
            insert(IntraHostVariant)
            .values(
                variants_slice
                .select([pl.col(cn) for cn in all_columns])
                .to_dicts()
            )
        )

        async with get_async_session() as session:
            await session.execute(
                base_insert.on_conflict_do_update(
                    constraint=ConstraintNames.uq_intra_host_variants_sample_allele_pair,
                    set_={k: getattr(base_insert.excluded, k) for k in update_columns}
                )
            )
            await session.commit()
