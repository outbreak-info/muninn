import polars as pl
from sqlalchemy import select, and_

from DB.engine import get_async_session
from DB.models import Allele
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

    async with GatheringTaskGroup() as tg:
        for row in alleles.iter_rows(named=True):
            tg.create_task(
                find_or_insert_allele(
                    Allele(
                        region=row[region_name],
                        position_nt=row[position_nt_name],
                        ref_nt=row[ref_nt_name],
                        alt_nt=row[alt_nt_name]
                    )
                )
            )
    alleles = alleles.with_columns(
        pl.Series('allele_id', tg.results())
    )
    return alleles
