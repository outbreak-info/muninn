from typing import Set, List

from sqlalchemy.sql.expression import text

from DB.engine import get_async_session
from utils.constants import StandardColumnNames, TableNames


async def get_aa_ids_for_annotation_effect(effect_id: int) -> List[Set[int]]:
    async with get_async_session() as session:
        res = await session.execute(
            text(
                f'''
                select array_agg(aaa.{StandardColumnNames.amino_acid_id}) 
                from {TableNames.annotations_amino_acids} aaa
                inner join {TableNames.annotations} a on a.id = aaa.{StandardColumnNames.annotation_id}
                inner join {TableNames.effects} e on e.id = a.{StandardColumnNames.effect_id}
                where e.id = :e_id
                group by aaa.{StandardColumnNames.annotation_id}
                '''
            ),
            {
               'e_id': effect_id
            }
        )
    return [set(r[0]) for r in res]


