from typing import List, Dict
from parser.parser import parser

from sqlalchemy import text
from DB.engine import get_async_session


async def get_annotations_by_substitution(
        region:str,
        substitution:str,
        raw_query:str | None = None
) -> Dict[str,List[str]]:
    user_where_clause = ''
    if raw_query is not None:
        user_where_clause = f'and ({parser.parse(raw_query)})'

    ref_aa = substitution[0]
    position_aa = substitution[1:-1]
    alt_aa = substitution[-1]

    async with get_async_session() as session:
        res = await session.execute(
            text(
                f'''
                select detail,
                    p.*
                from alleles 
                left join translations on translations.allele_id = alleles.id
                inner join annotations a on a.amino_acid_substitution_id = translations.amino_acid_substitution_id
                left join effects on effect_id = effects.id
                left join amino_acid_substitutions on a.amino_acid_substitution_id = amino_acid_substitutions.id
                left join annotations_papers ap on ap.annotation_id = a.id
                left join papers p on p.id = ap.paper_id
                where region = '{region}' and ref_aa = '{ref_aa}' and position_aa = '{position_aa}' and alt_aa = '{alt_aa}' {user_where_clause}
                group by detail,p.id
                ;
                '''
            )
        )
        effects = {}
        for r in res:
            if not effects.get(r[0]):
                effects[r[0]] = []
            effects[r[0]].append(f'{r[3]} {r[4]}')
        return effects