from typing import Dict, List

from sqlalchemy import select, text
from sqlalchemy.orm import contains_eager

from DB.engine import get_async_session
from DB.models import Sample, IntraHostVariant, Allele, AminoAcid, GeoLocation, IntraHostTranslation
from DB.queries.date_count_helpers import get_extract_clause, get_group_by_clause, get_order_by_cause, \
    MID_COLLECTION_DATE_CALCULATION
from DB.queries.helpers import get_ih_table_and_change_cols
from api.models import VariantNucleotideInfo, VariantAminoAcidInfo
from parser.parser import parser
from utils.constants import ColumnNames, DateBinOpt, NtOrAa, TableNames, COLLECTION_DATE


async def get_variants(
    change_bin: NtOrAa = NtOrAa.nt,
    filter: str = ""
) -> List['VariantNucleotideInfo'] | List['VariantAminoAcidInfo']:
    user_defined_filter = parser.parse(filter)

    if change_bin == NtOrAa.nt:
        variants_query = f'''
            select
                ihv.{ColumnNames.sample_id},
                ihv.{ColumnNames.allele_id},
                a.region,
                a.position_nt,
                a.ref_nt,
                a.alt_nt,
                ihv.ref_dp,
                ihv.alt_dp,
                ihv.alt_freq
            from {TableNames.intra_host_variants} ihv
            inner join {TableNames.alleles} a on a.id = ihv.{ColumnNames.allele_id}
            where {user_defined_filter}
        '''
        async with get_async_session() as session:
            result = await session.execute(text(variants_query))
            return [VariantNucleotideInfo(**row) for row in result.mappings().all()]
    else:
        variants_query = f'''
            select
                iht.{ColumnNames.sample_id},
                aa.position_aa,
                aa.ref_aa,
                aa.alt_aa,
                aa.gff_feature,
                aa.ref_codon,
                aa.alt_codon
            from {TableNames.intra_host_translations} iht
            inner join {TableNames.amino_acids} aa on aa.id = iht.{ColumnNames.amino_acid_id}
            where {user_defined_filter}
        '''
        async with get_async_session() as session:
            result = await session.execute(text(variants_query))
            return [VariantAminoAcidInfo(**row) for row in result.mappings().all()]


async def get_variants_by_sample(
    change_bin: NtOrAa = NtOrAa.nt,
    filter: str = ""
) -> List['VariantNucleotideInfo'] | List['VariantAminoAcidInfo']:
    user_defined_filter = parser.parse(filter)

    matching_samples = f'''
        select s.id
        from {TableNames.samples} s
        left join {TableNames.geo_locations} g on g.id = s.{ColumnNames.geo_location_id}
        where {user_defined_filter}
    '''

    if change_bin == NtOrAa.nt:
        variants_query = f'''
            select
                ihv.{ColumnNames.sample_id},
                ihv.{ColumnNames.allele_id},
                a.region,
                a.position_nt,
                a.ref_nt,
                a.alt_nt,
                ihv.ref_dp,
                ihv.alt_dp,
                ihv.alt_freq
            from {TableNames.intra_host_variants} ihv
            inner join {TableNames.alleles} a on a.id = ihv.{ColumnNames.allele_id}
            where ihv.{ColumnNames.sample_id} in (
                {matching_samples}
            )
        '''
        async with get_async_session() as session:
            result = await session.execute(text(variants_query))
            return [VariantNucleotideInfo(**row) for row in result.mappings().all()]
    else:
        variants_query = f'''
            select
                iht.{ColumnNames.sample_id},
                aa.position_aa,
                aa.ref_aa,
                aa.alt_aa,
                aa.gff_feature,
                aa.ref_codon,
                aa.alt_codon
            from {TableNames.intra_host_translations} iht
            inner join {TableNames.amino_acids} aa on aa.id = iht.{ColumnNames.amino_acid_id}
            where iht.{ColumnNames.sample_id} in (
                {matching_samples}
            )
        '''
        async with get_async_session() as session:
            result = await session.execute(text(variants_query))
            return [VariantAminoAcidInfo(**row) for row in result.mappings().all()]


async def get_variant_frequency_by_collection_date(
    date_bin: DateBinOpt,
    change_bin: NtOrAa,
    days: int,
    max_span_days: int,
    filter: str | None = None
) -> List[Dict]:
    ih_table, change_id_col, catalog_table, feature_col, ref_col, pos_col, alt_col = \
        get_ih_table_and_change_cols(change_bin)

    user_where_clause = ''
    if filter is not None:
        user_where_clause = f'and ({parser.parse(filter)})'

    extract_clause = get_extract_clause(COLLECTION_DATE, date_bin, days)
    group_by_clause = get_group_by_clause(
        date_bin,
        prefix_cols=[feature_col, ref_col, pos_col, alt_col]
    )
    order_by_clause = get_order_by_cause(date_bin)

    async with get_async_session() as session:
        res = await session.execute(
            text(
                f'''
                with sample_subset as (
                    select distinct
                        s.id as {ColumnNames.sample_id},
                        s.collection_start_date,
                        s.collection_end_date
                    from {TableNames.samples} s
                    left join {TableNames.geo_locations} gl on gl.id = s.{ColumnNames.geo_location_id}
                    left join samples_lineages sl on sl.{ColumnNames.sample_id} = s.id
                    left join lineages l on l.id = sl.lineage_id
                    left join lineage_systems ls on ls.id = l.lineage_system_id
                    where num_nulls(s.collection_end_date, s.collection_start_date) = 0
                        and s.collection_end_date - s.collection_start_date <= {max_span_days}
                        {user_where_clause}
                ),
                sample_subset_bm as (
                    select coalesce(rb_build_agg({ColumnNames.sample_id}), rb_build('{{}}')) as bm
                    from sample_subset
                )
                select
                    {extract_clause},
                    count(distinct {ColumnNames.sample_id}) as n,
                    percentile_cont(0.25) within group (order by alt_freq) as q1,
                    percentile_cont(0.5) within group (order by alt_freq) as median,
                    percentile_cont(0.75) within group (order by alt_freq) as q3,
                    {feature_col},
                    {ref_col},
                    {pos_col},
                    {alt_col}
                from (
                    select
                        ss.{ColumnNames.sample_id},
                        c.{feature_col},
                        c.{ref_col},
                        c.{pos_col},
                        c.{alt_col},
                        -- every observation in a bin is represented by that bin's midpoint
                        -- which makes these quartiles bin-resolution approximations
                        (((lower(v.alt_freq_range) + upper(v.alt_freq_range)) / 2))::double precision as alt_freq,
                        {MID_COLLECTION_DATE_CALCULATION}
                    from {ih_table} v
                    inner join {catalog_table} c on c.id = v.{change_id_col}
                    cross join lateral unnest(
                        rb_to_array(v.{ColumnNames.samples_present} & (select bm from sample_subset_bm))
                    ) as u({ColumnNames.sample_id})
                    inner join sample_subset ss on ss.{ColumnNames.sample_id} = u.{ColumnNames.sample_id}
                )
                {group_by_clause}
                {order_by_clause}
                '''
            )
        )
    out_data = []
    for r in res:
        date = date_bin.format_iso_chunk(r[0], r[1])
        out_data.append(
            {
                "date": date,
                "n": r[2],
                "alt_freq_q1": r[3],
                "alt_freq_median": r[4],
                "alt_freq_q3": r[5],
                feature_col: r[6],
                ref_col: r[7],
                pos_col: r[8],
                alt_col: r[9]
            }
        )
    return out_data
