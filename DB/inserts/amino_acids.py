from sqlalchemy import and_, select

from DB.engine import get_async_session
from DB.models import AminoAcid
from utils.errors import NotFoundError


# todo: should this include alt codon??
async def find_amino_acid(aa: AminoAcid) -> int:
    if None in {aa.alt_aa, aa.ref_aa, aa.position_aa, aa.gff_feature}:
        raise ValueError('Required fields absent from amino acid')

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


async def find_equivalent_amino_acids(aa: AminoAcid) -> set[int]:
    if None in {aa.alt_aa, aa.ref_aa, aa.position_aa, aa.gff_feature}:
        raise ValueError('Required fields absent from amino acid')

    async with get_async_session() as session:
        res = await session.scalars(
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
    ids = set(res.all())
    if len(ids) == 0:
        raise NotFoundError('No amino acids found')
    return ids