import polars as pl
from sqlalchemy import and_, select, insert

from DB.engine import get_async_session
from DB.models import AminoAcidSubstitution
from utils.errors import NotFoundError
from utils.gathering_task_group import GatheringTaskGroup


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
                    AminoAcidSubstitution.ref_aa == aas.ref_aa
                )
            )
        )
    if id_ is None:
        raise NotFoundError('No amino sub found')
    return id_


async def batch_insert_aa_subs(
    amino_subs: pl.DataFrame,
    position_aa_name: str = 'position_aa',
    ref_aa_name: str = 'ref_aa',
    alt_aa_name: str = 'alt_aa',
    gff_feature_name: str = 'gff_feature',
    ref_codon_name: str = 'ref_codon',
    alt_codon_name: str = 'alt_codon',
) -> pl.DataFrame:

    async with get_async_session() as session:
        ids = await session.scalars(
            insert(AminoAcidSubstitution).returning(AminoAcidSubstitution.id),
            amino_subs.to_dicts()
        )
        await session.commit()
    amino_subs = amino_subs.with_columns(
        pl.Series('amino_acid_substitution_id', ids)
    )
    return amino_subs
