from sqlalchemy import and_, select

from DB.engine import get_async_session
from DB.models import AminoAcid
from DB.textutils import text
from utils.constants import TableNames, ColumnNames
from utils.errors import NotFoundError


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


async def find_equivalent_amino_acids_with_gff_pattern(
    gff_pattern: str,
    ref_aa: str,
    position_aa: int,
    alt_aa: str
) -> set[int]:
    if None in {alt_aa, ref_aa, position_aa, gff_pattern}:
        raise ValueError('Required amino acid data absent')

    async with get_async_session() as session:
        res = await session.scalars(
            text(
                f'''
                select id
                from {TableNames.amino_acids}
                where {ColumnNames.gff_feature} ~ :gff_pattern
                      and {ColumnNames.ref_aa} = :ref_aa
                      and {ColumnNames.position_aa} = :position_aa
                      and {ColumnNames.alt_aa} = :alt_aa
                '''
            ),
            {
                'gff_pattern': gff_pattern,
                'ref_aa': ref_aa,
                'position_aa': position_aa,
                'alt_aa': alt_aa
            }
        )
    ids = set(res.all())
    if len(ids) == 0:
        raise NotFoundError('No amino acids found')
    return ids
