from typing import Set, List

import polars as pl
from sqlalchemy import select, and_
from sqlalchemy.sql.expression import text

from DB.engine import get_uri_for_polars, get_async_session
from DB.models import AminoAcid
from utils.constants import StandardColumnNames, TableNames
from utils.errors import NotFoundError


async def get_all_amino_acid_subs_as_pl_df() -> pl.DataFrame:
    return pl.read_database_uri(
        query=f'select * from {TableNames.amino_acids};',
        uri=get_uri_for_polars()
    ).rename({'id': StandardColumnNames.amino_acid_id})


async def find_aa_sub(aas: AminoAcid) -> int:
    if None in {aas.alt_aa, aas.ref_aa, aas.position_aa, aas.gff_feature}:
        raise RuntimeError('Required fields absent from aas')

    async with get_async_session() as session:
        id_ = await session.scalar(
            select(AminoAcid.id)
            .where(
                and_(
                    AminoAcid.gff_feature == aas.gff_feature,
                    AminoAcid.position_aa == aas.position_aa,
                    AminoAcid.alt_aa == aas.alt_aa,
                    AminoAcid.ref_aa == aas.ref_aa
                )
            )
        )
    if id_ is None:
        raise NotFoundError('No amino sub found')
    return id_


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


