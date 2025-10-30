import polars as pl
from sqlalchemy import select, and_

from DB.engine import get_uri_for_polars, get_async_session
from DB.models import Sample, AminoAcid
from utils.constants import StandardColumnNames, TableNames
from utils.errors import NotFoundError

# this is a home for queries whose only use is to assist in data ingestion

async def get_all_lineages_immediate_children_by_system_as_pl_df(lineage_system_name: str) -> pl.DataFrame:
    return pl.read_database_uri(
        query=f'''
        select 
            {StandardColumnNames.parent_id},
            {StandardColumnNames.child_id}
        from {TableNames.lineages_immediate_children} lid
        -- we only need to check the parent, they have to be on the same system
        inner join {TableNames.lineages} l on l.id = lid.{StandardColumnNames.parent_id}
        inner join {TableNames.lineage_systems} ls on ls.id = l.{StandardColumnNames.lineage_system_id}
        where ls.{StandardColumnNames.lineage_system_name} = '{lineage_system_name}'
        ''',
        uri=get_uri_for_polars()
    )


async def get_samples_accession_and_id_as_pl_df() -> pl.DataFrame:
    return pl.read_database_uri(
        query=f'select id, accession from samples;',
        uri=get_uri_for_polars()
    ).rename({'id': StandardColumnNames.sample_id})


async def get_sample_id_by_accession(accession: str) -> int:
    async with get_async_session() as session:
        id_ = await session.scalar(
            select(Sample.id)
            .where(Sample.accession == accession)
        )
    if id_ is None:
        raise NotFoundError(f'No sample found for accession: {accession}')
    return id_


async def get_all_lineages_by_lineage_system_as_pl_df(lineage_system_name: str) -> pl.DataFrame:
    return pl.read_database_uri(
        query=f'''
        select 
            l.id as {StandardColumnNames.lineage_id},
            l.{StandardColumnNames.lineage_name}
        from {TableNames.lineages} l
        inner join {TableNames.lineage_systems} ls on ls.id = l.{StandardColumnNames.lineage_system_id}
        where ls.{StandardColumnNames.lineage_system_name} = '{lineage_system_name}'
        ''',
        uri=get_uri_for_polars()
    )


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
