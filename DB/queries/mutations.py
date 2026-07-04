from typing import List

from sqlalchemy import select, text
from sqlalchemy.orm import contains_eager

from DB.engine import get_async_session
from DB.models import Mutation, Allele, AminoAcid, Sample, GeoLocation, MutationTranslation
from DB.queries.date_count_helpers import get_extract_clause, MID_COLLECTION_DATE_CALCULATION, get_order_by_cause, \
    get_group_by_clause
from api.models import MutationInfo
from parser.parser import parser
from utils.constants import StandardColumnNames, DateBinOpt, TableNames, COLLECTION_DATE


async def get_mutations(query: str) -> List['MutationInfo']:
    user_query = parser.parse(query)

    mutations_query = (
        select(Mutation, Allele, MutationTranslation, AminoAcid)
        .join(Allele, Mutation.allele_id == Allele.id, isouter=True)
        .options(contains_eager(Mutation.r_allele))
        .join(MutationTranslation, MutationTranslation.mutation_id == Mutation.id, isouter=True)
        .options(contains_eager(Mutation.r_translations))
        .join(AminoAcid, AminoAcid.id == MutationTranslation.amino_acid_id, isouter=True)
        .options(contains_eager(MutationTranslation.r_amino_acid))
        .where(
            text(user_query)
        )
    )

    async with get_async_session() as session:
        results = await session.scalars(mutations_query)
        out_data = [MutationInfo.from_db_object(m) for m in results.unique()]
    return out_data


async def get_mutations_by_sample(query: str) -> List['MutationInfo']:
    user_query = parser.parse(query)

    mutations_query = (
        select(Mutation, Allele, MutationTranslation, AminoAcid)
        .join(Allele, Mutation.allele_id == Allele.id, isouter=True)
        .options(contains_eager(Mutation.r_allele))
        .join(MutationTranslation, MutationTranslation.mutation_id == Mutation.id, isouter=True)
        .options(contains_eager(Mutation.r_translations))
        .join(AminoAcid, AminoAcid.id == MutationTranslation.amino_acid_id, isouter=True)
        .options(contains_eager(MutationTranslation.r_amino_acid))
        .where(
            Mutation.sample_id.in_(
                select(Sample.id)
                .join(GeoLocation, GeoLocation.id == Sample.geo_location_id, isouter=True)
                .where(text(user_query))
            )
        )
    )

    async with get_async_session() as session:
        results = await session.scalars(mutations_query)
        out_data = [MutationInfo.from_db_object(m) for m in results.unique()]
    return out_data


async def get_aa_mutation_count_by_collection_date(
    date_bin: DateBinOpt,
    position_aa: int,
    alt_aa: str,
    gff_feature: str,
    days: int,
    max_span_days: int,
    raw_query: str
):
    user_where_clause = ''
    if raw_query is not None:
        user_where_clause = f'where ({parser.parse(raw_query)})'

    extract_clause = get_extract_clause(COLLECTION_DATE, date_bin, days)
    group_by_clause = get_group_by_clause(
        date_bin,
        prefix_cols=[
            StandardColumnNames.gff_feature,
            StandardColumnNames.position_aa,
            StandardColumnNames.alt_aa,
            StandardColumnNames.ref_aa,
            StandardColumnNames.lineage_name
        ]
    )
    order_by_clause = get_order_by_cause(date_bin)

    async with get_async_session() as session:
        res = await session.execute(
            text(
                f'''
                with translation_sequences as (
                    select
                        aa.gff_feature,
                        aa.ref_aa,
                        aa.position_aa,
                        aa.alt_aa,
                        seqs.sequence_id as target_sequence_id
                    from {TableNames.amino_acids} aa
                    inner join {TableNames.mutation_translations} t on t.{StandardColumnNames.amino_acid_id} = aa.id
                    cross join lateral unnest(rb_to_array(t.{StandardColumnNames.sequences_present})) as seqs(sequence_id)
                    where aa.position_aa = {position_aa} and aa.alt_aa = :alt_aa and aa.gff_feature = :gff_feature
                )
                select
                {extract_clause},
                count(distinct sample_id) as n,
                gff_feature,
                ref_aa,
                position_aa,
                alt_aa,
                lineage_name
                from(
                    select
                    gff_feature,
                    ref_aa,
                    position_aa,
                    alt_aa,
                    sample_id,
                    lineage_name,
                    {MID_COLLECTION_DATE_CALCULATION}
                    from (
                        select
                            ts.gff_feature,
                            ts.ref_aa,
                            ts.position_aa,
                            ts.alt_aa,
                            s.id as sample_id,
                            s.collection_start_date,
                            s.collection_end_date,
                            l.lineage_name,
                            collection_end_date - collection_start_date as collection_span
                        from translation_sequences ts
                        inner join {TableNames.samples} s on s.{StandardColumnNames.sequence_id} = ts.target_sequence_id
                        inner join {TableNames.samples_lineages} sl on sl.sample_id = s.id
                        inner join {TableNames.lineages} l on l.id = sl.lineage_id
                        {user_where_clause}
                    )
                    where collection_span <= {max_span_days}
                )
                {group_by_clause}
                {order_by_clause}
                '''
            ),
            {
                'alt_aa': alt_aa,
                'gff_feature': gff_feature
            }
        )
    out_data = []
    for r in res:
        date = date_bin.format_iso_chunk(r[0], r[1])
        out_data.append(
            {
                "date": date,
                "n": r[2],
                "gff_feature": r[3],
                "ref_aa": r[4],
                "position_aa": r[5],
                "alt_aa": r[6],
                "lineage_name": r[7]
            }
        )
    return out_data


async def get_nt_mutation_count_by_collection_date(
    date_bin: DateBinOpt,
    position_nt: int,
    alt_nt: str,
    region: str,
    days: int,
    max_span_days: int,
    raw_query: str | None = None
):
    user_where_clause = ''
    if raw_query is not None:
        user_where_clause = f'where ({parser.parse(raw_query)})'

    extract_clause = get_extract_clause(COLLECTION_DATE, date_bin, days)
    group_by_clause = get_group_by_clause(
        date_bin,
        prefix_cols=[
            StandardColumnNames.region,
            StandardColumnNames.position_nt,
            StandardColumnNames.alt_nt,
            StandardColumnNames.ref_nt,
            StandardColumnNames.lineage_name
        ]
    )
    order_by_clause = get_order_by_cause(date_bin)

    async with get_async_session() as session:
        res = await session.execute(
            text(
                f'''
                with mutation_sequences as (
                    select
                        a.region,
                        a.ref_nt,
                        a.position_nt,
                        a.alt_nt,
                        seqs.sequence_id as target_sequence_id
                    from {TableNames.alleles} a
                    inner join {TableNames.mutations} m on m.{StandardColumnNames.allele_id} = a.id
                    cross join lateral unnest(rb_to_array(m.{StandardColumnNames.sequences_present})) as seqs(sequence_id)
                    where a.position_nt = {position_nt} and a.alt_nt = :alt_nt and a.region = :region
                )
                select
                {extract_clause},
                count(distinct sample_id) as n,
                region,
                ref_nt,
                position_nt,
                alt_nt,
                lineage_name
                from(
                    select
                    region,
                    ref_nt,
                    position_nt,
                    alt_nt,
                    sample_id,
                    lineage_name,
                    {MID_COLLECTION_DATE_CALCULATION}
                    from (
                        select
                            ms.region,
                            ms.ref_nt,
                            ms.position_nt,
                            ms.alt_nt,
                            s.id as sample_id,
                            s.collection_start_date,
                            s.collection_end_date,
                            l.lineage_name,
                            collection_end_date - collection_start_date as collection_span
                        from mutation_sequences ms
                        inner join {TableNames.samples} s on s.{StandardColumnNames.sequence_id} = ms.target_sequence_id
                        inner join {TableNames.samples_lineages} sl on sl.sample_id = s.id
                        inner join {TableNames.lineages} l on l.id = sl.lineage_id
                        {user_where_clause}
                    )
                    where collection_span <= {max_span_days}
                )
                {group_by_clause}
                {order_by_clause}
                '''
            ),
            {
                'alt_nt': alt_nt,
                'region': region
            }
        )
    # print(res.all())
    out_data = []
    for r in res:
        date = date_bin.format_iso_chunk(r[0], r[1])
        out_data.append(
            {
                "date": date,
                "n": r[2],
                "region": r[3],
                "ref_nt": r[4],
                "position_nt": r[5],
                "alt_nt": r[6],
                "lineage_name": r[7]
            }
        )
    return out_data
