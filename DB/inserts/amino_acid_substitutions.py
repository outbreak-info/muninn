import polars as pl
from sqlalchemy import and_, select

from DB.engine import get_async_write_session, get_asyncpg_connection
from DB.models import AminoAcid
from utils.constants import StandardColumnNames, TableNames


async def find_or_insert_aa_sub(aas: AminoAcid) -> int:
    async with get_async_write_session() as session:
        id_: int = await session.scalar(
            select(AminoAcid.id)
            .where(
                and_(
                    AminoAcid.gff_feature == aas.gff_feature,
                    AminoAcid.position_aa == aas.position_aa,
                    AminoAcid.alt_aa == aas.alt_aa
                )
            )
        )
        if id_ is None:
            session.add(aas)
            await session.commit()
            await session.refresh(aas)
            id_ = aas.id
    return id_


<<<<<<< HEAD
async def copy_insert_aa_subs(aa_subs: pl.DataFrame) -> str:
    columns = [
        StandardColumnNames.gff_feature,
        StandardColumnNames.position_aa,
        StandardColumnNames.ref_aa,
        StandardColumnNames.alt_aa,
    ]
    conn = await get_asyncpg_connection()
    res = await conn.copy_records_to_table(
        TableNames.amino_acids,
        records=aa_subs.select(
            [pl.col(cn) for cn in columns]
        ).iter_rows(),
        columns=columns
    )
    return res
=======
async def find_aa_sub(aas: AminoAcidSubstitution) -> int:
    if None in {aas.alt_aa, aas.ref_aa, aas.position_aa, aas.gff_feature}:
        raise RuntimeError('Required fields absent from aas')

    async with get_async_session() as session:
        id_ = await session.scalar(
            select(AminoAcidSubstitution.id)
            .where(
                and_(
                    AminoAcidSubstitution.gff_feature == aas.gff_feature,
                    AminoAcidSubstitution.position_aa == aas.position_aa,
                    AminoAcidSubstitution.alt_aa == aas.alt_aa,
                    AminoAcidSubstitution.ref_aa == aas.ref_aa
                )
            )
        )
    if id_ is None:
        raise NotFoundError('No amino sub found')
    return id_

async def insert_aa_sub(aas: AminoAcidSubstitution) -> int:
    async with get_async_session() as session:
        session.add(aas)
        await session.commit()
        await session.refresh(aas)
        id_ = aas.id
    return id_
>>>>>>> annotations
