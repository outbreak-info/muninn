from sqlalchemy import and_, select

from DB.engine import get_async_write_session, get_async_session
from DB.models import AminoAcid
from utils.errors import NotFoundError


async def find_or_insert_amino_acid(aa: AminoAcid) -> int:
    async with get_async_write_session() as session:
        id_: int = await session.scalar(
            select(AminoAcid.id)
            .where(
                and_(
                    AminoAcid.gff_feature == aa.gff_feature,
                    AminoAcid.position_aa == aa.position_aa,
                    AminoAcid.alt_aa == aa.alt_aa
                )
            )
        )
        if id_ is None:
            session.add(aa)
            await session.commit()
            await session.refresh(aa)
            id_ = aa.id
    return id_


async def find_amino_acid(aa: AminoAcid) -> int:
    if None in {aa.alt_aa, aa.ref_aa, aa.position_aa, aa.gff_feature}:
        raise RuntimeError('Required fields absent from amino acid')

    async with get_async_session() as session:
        id_ = await session.scalar(
            select(AminoAcid.id)
            .where(
                and_(
                    AminoAcid.gff_feature == aa.gff_feature,
                    AminoAcid.position_aa == aa.position_aa,
                    AminoAcid.alt_aa == aa.alt_aa,
                    AminoAcid.ref_aa == aa.ref_aa
                )
            )
        )
    if id_ is None:
        raise NotFoundError('No amino acid found')
    return id_
