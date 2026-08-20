from typing import List

from sqlalchemy import text

from DB.engine import get_async_session
from DB.queries.date_count_helpers import get_extract_clause, MID_COLLECTION_DATE_CALCULATION, get_order_by_cause, \
    get_group_by_clause
from api.models import MutationNucleotideInfo, MutationAminoAcidInfo
from parser.parser import parser
from utils.constants import ColumnNames, DateBinOpt, TableNames, COLLECTION_DATE, NtOrAa


async def get_mutations(
    change_bin: NtOrAa = NtOrAa.nt,
    where: str = ""
) -> List['MutationNucleotideInfo'] | List['MutationAminoAcidInfo']:
    user_where_clause = parser.parse(where)

    if change_bin == NtOrAa.nt:
        mutations_query = f'''
            select
                samps.sample_id,
                m.{ColumnNames.allele_id},
                a.region,
                a.position_nt,
                a.ref_nt,
                a.alt_nt
            from {TableNames.cns_samples_by_allele} m
            inner join {TableNames.alleles} a on a.id = m.{ColumnNames.allele_id}
            cross join lateral unnest(rb_to_array(m.{ColumnNames.samples_present})) as samps(sample_id)
            where {user_where_clause}
        '''
        async with get_async_session() as session:
            result = await session.execute(text(mutations_query))
            return [MutationNucleotideInfo(**row) for row in result.mappings().all()]
    else:
        mutations_query = f'''
            select
                samps.sample_id,
                aa.position_aa,
                aa.ref_aa,
                aa.alt_aa,
                aa.gff_feature,
                aa.ref_codon,
                aa.alt_codon
            from {TableNames.cns_samples_by_amino_acid} mt
            inner join {TableNames.amino_acids} aa on aa.id = mt.{ColumnNames.amino_acid_id}
            cross join lateral unnest(rb_to_array(mt.{ColumnNames.samples_present})) as samps(sample_id)
            where {user_where_clause}
        '''
        async with get_async_session() as session:
            result = await session.execute(text(mutations_query))
            return [MutationAminoAcidInfo(**row) for row in result.mappings().all()]


async def get_mutations_by_sample(
    change_bin: NtOrAa = NtOrAa.nt,
    where: str = ""
) -> List['MutationNucleotideInfo'] | List['MutationAminoAcidInfo']:
    user_where_clause = parser.parse(where)

    matching_samples = f'''
        select s.id
        from {TableNames.samples} s
        left join {TableNames.geo_locations} g on g.id = s.{ColumnNames.geo_location_id}
        where {user_where_clause}
    '''

    if change_bin == NtOrAa.nt:
        mutations_query = f'''
            select
                cabs.{ColumnNames.sample_id},
                alls.allele_id,
                a.region,
                a.position_nt,
                a.ref_nt,
                a.alt_nt
            from {TableNames.cns_alleles_by_sample} cabs
            cross join lateral unnest(rb_to_array(cabs.{ColumnNames.alleles_present})) as alls(allele_id)
            inner join {TableNames.alleles} a on a.id = alls.allele_id
            where cabs.{ColumnNames.sample_id} in (
                {matching_samples}
            )
        '''
        async with get_async_session() as session:
            result = await session.execute(text(mutations_query))
            return [MutationNucleotideInfo(**row) for row in result.mappings().all()]
    else:
        mutations_query = f'''
            select
                caabs.{ColumnNames.sample_id},
                aa.position_aa,
                aa.ref_aa,
                aa.alt_aa,
                aa.gff_feature,
                aa.ref_codon,
                aa.alt_codon
            from {TableNames.cns_amino_acids_by_sample} caabs
            cross join lateral unnest(rb_to_array(caabs.{ColumnNames.amino_acids_present})) as aas(amino_acid_id)
            inner join {TableNames.amino_acids} aa on aa.id = aas.amino_acid_id
            where caabs.{ColumnNames.sample_id} in (
                {matching_samples}
            )
        '''
        async with get_async_session() as session:
            result = await session.execute(text(mutations_query))
            return [MutationAminoAcidInfo(**row) for row in result.mappings().all()]


async def get_aa_mutation_count_by_collection_date(
    date_bin: DateBinOpt,
    position_aa: int,
    alt_aa: str,
    gff_feature: str,
    days: int,
    max_span_days: int,
    where: str | None = None
):
    user_where_clause = ''
    if where is not None:
        user_where_clause = f'where ({parser.parse(where)})'

    extract_clause = get_extract_clause(COLLECTION_DATE, date_bin, days)
    group_by_clause = get_group_by_clause(
        date_bin,
        prefix_cols=[
            ColumnNames.gff_feature,
            ColumnNames.position_aa,
            ColumnNames.alt_aa,
            ColumnNames.ref_aa,
            ColumnNames.lineage_name
        ]
    )
    order_by_clause = get_order_by_cause(date_bin)

    async with get_async_session() as session:
        res = await session.execute(
            text(
                f'''
                with translation_samples as (
                    select
                        aa.gff_feature,
                        aa.ref_aa,
                        aa.position_aa,
                        aa.alt_aa,
                        samps.{ColumnNames.sample_id} as target_sample_id
                    from {TableNames.amino_acids} aa
                    inner join {TableNames.cns_samples_by_amino_acid} t on t.{ColumnNames.amino_acid_id} = aa.id
                    cross join lateral unnest(rb_to_array(t.{ColumnNames.samples_present})) as samps({ColumnNames.sample_id})
                    where aa.position_aa = :position_aa and aa.alt_aa = :alt_aa and aa.gff_feature = :gff_feature
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
                        from translation_samples ts
                        inner join {TableNames.samples} s on s.id = ts.target_sample_id
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
                'position_aa': position_aa,
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
    where: str | None = None
):
    user_where_clause = ''
    if where is not None:
        user_where_clause = f'where ({parser.parse(where)})'

    extract_clause = get_extract_clause(COLLECTION_DATE, date_bin, days)
    group_by_clause = get_group_by_clause(
        date_bin,
        prefix_cols=[
            ColumnNames.region,
            ColumnNames.position_nt,
            ColumnNames.alt_nt,
            ColumnNames.ref_nt,
            ColumnNames.lineage_name
        ]
    )
    order_by_clause = get_order_by_cause(date_bin)

    async with get_async_session() as session:
        res = await session.execute(
            text(
                f'''
                with mutation_samples as (
                    select
                        a.region,
                        a.ref_nt,
                        a.position_nt,
                        a.alt_nt,
                        samps.{ColumnNames.sample_id} as target_sample_id
                    from {TableNames.alleles} a
                    inner join {TableNames.cns_samples_by_allele} m on m.{ColumnNames.allele_id} = a.id
                    cross join lateral unnest(rb_to_array(m.{ColumnNames.samples_present})) as samps({ColumnNames.sample_id})
                    where a.position_nt = :position_nt and a.alt_nt = :alt_nt and a.region = :region
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
                        from mutation_samples ms
                        inner join {TableNames.samples} s on s.id = ms.target_sample_id
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
                'position_nt': position_nt,
                'alt_nt': alt_nt,
                'region': region
            }
        )
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
