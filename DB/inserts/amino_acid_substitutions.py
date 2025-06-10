from sqlalchemy import and_, select

from DB.engine import get_async_session
from DB.models import AminoAcidSubstitution
from utils.errors import NotFoundError


async def find_or_insert_aa_sub(aas: AminoAcidSubstitution) -> int:
    async with get_async_session() as session:
        id_: int = await session.scalar(
            select(AminoAcidSubstitution.id)
            .where(
                and_(
                    AminoAcidSubstitution.gff_feature == aas.gff_feature,
                    AminoAcidSubstitution.position_aa == aas.position_aa,
                    AminoAcidSubstitution.alt_aa == aas.alt_aa
                )
            )
        )
        if id_ is None:
            session.add(aas)
            await session.commit()
            await session.refresh(aas)
            id_ = aas.id
    return id_


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
