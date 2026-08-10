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
    filter: str = "",
    min_alt_freq: float | None = None,
    max_alt_freq: float | None = None
) -> List['VariantNucleotideInfo'] | List['VariantAminoAcidInfo']:
    user_defined_filter = parser.parse(filter)
    ih_table, change_id_col, catalog_table, *_ = get_ih_table_and_change_cols(change_bin)
    if change_bin == NtOrAa.nt:
        model = VariantNucleotideInfo
        change_columns = f'v.{ColumnNames.allele_id}, c.region, c.position_nt, c.ref_nt, c.alt_nt'
    else:
        model = VariantAminoAcidInfo
        change_columns = 'c.position_aa, c.ref_aa, c.alt_aa, c.gff_feature, c.ref_codon, c.alt_codon'

    variants_query = f'''
        select
            u.{ColumnNames.sample_id},
            {change_columns},
            v.alt_freq_range::text as alt_freq_range,
            lower(v.alt_freq_range)::double precision as alt_freq_lower,
            upper(v.alt_freq_range)::double precision as alt_freq_upper
        from {ih_table} v
        inner join {catalog_table} c on c.id = v.{change_id_col}
        cross join lateral unnest(rb_to_array(v.{ColumnNames.samples_present})) as u({ColumnNames.sample_id})
        where {user_defined_filter}
        and v.alt_freq_range && numrange(:min_alt_freq, :max_alt_freq, '[]')
    '''

    async with get_async_session() as session:
        result = await session.execute(
            text(variants_query),
            {'min_alt_freq': min_alt_freq, 'max_alt_freq': max_alt_freq}
        )
        return [model(**row) for row in result.mappings().all()]


async def get_variants_by_sample(
    change_bin: NtOrAa = NtOrAa.nt,
    filter: str = "",
    min_alt_freq: float | None = None,
    max_alt_freq: float | None = None
) -> List['VariantNucleotideInfo'] | List['VariantAminoAcidInfo']:
    user_defined_filter = parser.parse(filter)
    ih_table, change_id_col, catalog_table, *_ = get_ih_table_and_change_cols(change_bin)
    if change_bin == NtOrAa.nt:
        model = VariantNucleotideInfo
        change_columns = f'v.{ColumnNames.allele_id}, c.region, c.position_nt, c.ref_nt, c.alt_nt'
    else:
        model = VariantAminoAcidInfo
        change_columns = 'c.position_aa, c.ref_aa, c.alt_aa, c.gff_feature, c.ref_codon, c.alt_codon'

    variants_query = f'''
        with sample_subset_bm as (
            select coalesce(rb_build_agg(s.id), rb_build('{{}}')) as bm
            from {TableNames.samples} s
            left join {TableNames.geo_locations} gl on gl.id = s.{ColumnNames.geo_location_id}
            where {user_defined_filter}
        )
        select
            u.{ColumnNames.sample_id},
            {change_columns},
            v.alt_freq_range::text as alt_freq_range,
            lower(v.alt_freq_range)::double precision as alt_freq_lower,
            upper(v.alt_freq_range)::double precision as alt_freq_upper
        from {ih_table} v
        inner join {catalog_table} c on c.id = v.{change_id_col}
        cross join lateral unnest(
            rb_to_array(v.{ColumnNames.samples_present} & (select bm from sample_subset_bm))
        ) as u({ColumnNames.sample_id})
        where v.alt_freq_range && numrange(:min_alt_freq, :max_alt_freq, '[]')
    '''

    async with get_async_session() as session:
        result = await session.execute(
            text(variants_query),
            {'min_alt_freq': min_alt_freq, 'max_alt_freq': max_alt_freq}
        )
        return [model(**row) for row in result.mappings().all()]


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
