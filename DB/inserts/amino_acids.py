from DB.engine import get_async_session
from DB.textutils import text
from utils.constants import TableNames, ColumnNames
from utils.errors import NotFoundError


async def find_equivalent_amino_acids(
    gff_feature: str,
    ref_aa: str,
    position_aa: int,
    alt_aa: str
) -> set[int]:
    if None in {alt_aa, ref_aa, position_aa, gff_feature}:
        raise ValueError('Required fields absent from amino acid')

    async with get_async_session() as session:
        res = await session.scalars(
            text(
                f'''
                select id
                from {TableNames.amino_acids}
                where {ColumnNames.gff_feature} = :gff_feature
                      and {ColumnNames.ref_aa} = :ref_aa
                      and {ColumnNames.position_aa} = :position_aa
                      and {ColumnNames.alt_aa} = :alt_aa
                '''
            ),
            {
                'gff_feature': gff_feature,
                'ref_aa': ref_aa,
                'position_aa': position_aa,
                'alt_aa': alt_aa
            }
        )
    ids = set(res.all())
    if len(ids) == 0:
        raise NotFoundError('No amino acids found')
    return ids
