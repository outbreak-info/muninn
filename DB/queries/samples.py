from typing import List, Dict

from sqlalchemy import text

from DB.engine import get_async_session
from api.models import SampleInfo, LineageCountInfo
from parser.parser import parser
from DB.queries.date_count_helpers import get_extract_clause, get_group_by_clause, get_order_by_cause, \
    MID_COLLECTION_DATE_CALCULATION
from DB.queries.counts import count_samples_by_column, count_samples_by_simple_date, \
    count_samples_by_collection_date, count_lineages_by_simple_date, count_lineages_by_collection_date
from DB.queries.lineages import get_sample_counts_by_lineage
from DB.queries.helpers import get_ih_table_and_change_cols
from utils.constants import DateBinOpt, NtOrAa, TableNames, ColumnNames, COLLECTION_DATE, DEFAULT_DAYS, \
    DEFAULT_MAX_SPAN_DAYS, LINEAGE, SIMPLE_DATE_FIELDS


async def get_sample_by_id(sample_id: int) -> SampleInfo | None:
    query = f"""
    select s.*,
        g.country_name as geo_country_name,
        g.admin1_name as geo_admin1_name,
        g.admin2_name as geo_admin2_name,
        g.admin3_name as geo_admin3_name
    from {TableNames.samples} s
    left join {TableNames.geo_locations} g on g.id = s.{ColumnNames.geo_location_id}
    where s.id = :sample_id
    """

    async with get_async_session() as session:
        result = await session.execute(text(query), {'sample_id': sample_id})
        row = result.mappings().first()
    if row is None:
        return None
    return SampleInfo(**row)


async def get_samples(where: str) -> List['SampleInfo']:
    user_where_clause = parser.parse(where)

    query = f"""
    select s.*,
        g.country_name as geo_country_name,
        g.admin1_name as geo_admin1_name,
        g.admin2_name as geo_admin2_name,
        g.admin3_name as geo_admin3_name
    from {TableNames.samples} s
    left join {TableNames.geo_locations} g on g.id = s.{ColumnNames.geo_location_id}
    where {user_where_clause}
    """

    async with get_async_session() as session:
        samples = await session.execute(text(query))
        out_data = [SampleInfo(**row) for row in samples.mappings().all()]
    return out_data


async def get_samples_by_mutation(change_bin: NtOrAa = NtOrAa.aa, where: str = "") -> List['SampleInfo']:
    user_where_clause = parser.parse(where)

    if change_bin == NtOrAa.nt:
        matching_samples = f'''
            select samps.sample_id
            from {TableNames.cns_samples_by_allele} m
            inner join {TableNames.alleles} a on a.id = m.{ColumnNames.allele_id}
            cross join lateral unnest(rb_to_array(m.{ColumnNames.samples_present})) as samps(sample_id)
            where {user_where_clause}
        '''
    else:
        matching_samples = f'''
            select samps.sample_id
            from {TableNames.cns_samples_by_amino_acid} mt
            inner join {TableNames.amino_acids} aa on aa.id = mt.{ColumnNames.amino_acid_id}
            cross join lateral unnest(rb_to_array(mt.{ColumnNames.samples_present})) as samps(sample_id)
            where {user_where_clause}
        '''

    samples_query = f"""
    select s.*,
        g.country_name as geo_country_name,
        g.admin1_name as geo_admin1_name,
        g.admin2_name as geo_admin2_name,
        g.admin3_name as geo_admin3_name
    from {TableNames.samples} s
    left join {TableNames.geo_locations} g on g.id = s.{ColumnNames.geo_location_id}
    where s.id in (
        {matching_samples}
    )
    """

    async with get_async_session() as session:
        samples = await session.execute(text(samples_query))
        out_data = [SampleInfo(**row) for row in samples.mappings().all()]
    return out_data


async def get_samples_by_variant(
    change_bin: NtOrAa = NtOrAa.aa,
    where: str = "",
    min_alt_freq: float | None = None,
    max_alt_freq: float | None = None
) -> List['SampleInfo']:
    user_where_clause = parser.parse(where)
    ih_table, change_id_col, catalog_table, *_ = get_ih_table_and_change_cols(change_bin)

    samples_query = f"""
    with matching_bm as (
        select rb_or_agg(v.{ColumnNames.samples_present}) as bm
        from {ih_table} v
        inner join {catalog_table} c on c.id = v.{change_id_col}
        where {user_where_clause}
        and v.alt_freq_range && numrange(:min_alt_freq, :max_alt_freq, '[]')
    )
    select s.*,
        g.country_name as geo_country_name,
        g.admin1_name as geo_admin1_name,
        g.admin2_name as geo_admin2_name,
        g.admin3_name as geo_admin3_name
    from {TableNames.samples} s
    left join {TableNames.geo_locations} g on g.id = s.{ColumnNames.geo_location_id}
    where s.id = any(rb_to_array((select bm from matching_bm)))
    """

    async with get_async_session() as session:
        samples = await session.execute(
            text(samples_query),
            {'min_alt_freq': min_alt_freq, 'max_alt_freq': max_alt_freq}
        )
        return [SampleInfo(**row) for row in samples.mappings().all()]


async def get_sample_counts(
    group_by: str,
    date_bin: DateBinOpt = DateBinOpt.month,
    days: int = DEFAULT_DAYS,
    where: str | None = None,
    max_span_days: int = DEFAULT_MAX_SPAN_DAYS,
) -> Dict[str, int] | Dict[str, Dict[str, Dict[str, int]]] | List[LineageCountInfo]:
    group_by_set = set(group_by.split(','))
    if len(group_by_set) > 1:
        if len(group_by_set) > 2:
            raise ValueError('Max of 2 group_by values allowed')
        if LINEAGE in group_by_set:
            date_field = group_by_set.difference({LINEAGE}).pop()
            if date_field in SIMPLE_DATE_FIELDS:
                return await count_lineages_by_simple_date(date_field, date_bin, where, days)
            elif date_field == COLLECTION_DATE:
                return await count_lineages_by_collection_date(date_bin, where, days, max_span_days)

        raise NotImplementedError(
            'Grouping by multiple fields is currently only supported for lineage plus a date field'
        )
    else:
        if group_by in SIMPLE_DATE_FIELDS:
            return await count_samples_by_simple_date(group_by, date_bin, days, where)
        elif group_by == COLLECTION_DATE:
            return await count_samples_by_collection_date(date_bin, days, where, max_span_days)
        elif group_by == LINEAGE:
            return await get_sample_counts_by_lineage(where)
        else:
            return await count_samples_by_column(group_by, where)


async def get_sample_collection_release_lag(
    max_span_days: int,
    date_bin: DateBinOpt = DateBinOpt.month,
    days: int = DEFAULT_DAYS,
) -> List[Dict]:
    extract_clause = get_extract_clause(COLLECTION_DATE, date_bin, days)
    group_by_clause = get_group_by_clause(date_bin)
    order_by_clause = get_order_by_cause(date_bin)

    query = f"""
    select
        {extract_clause},
        percentile_cont(0.25) within group (order by lag) as q1,
        percentile_cont(0.5)  within group (order by lag) as median,
        percentile_cont(0.75) within group (order by lag) as q3
    from (
        select
            mid_collection_date,
            {ColumnNames.release_date}::date - mid_collection_date as lag
        from (
            select
                {MID_COLLECTION_DATE_CALCULATION},
                {ColumnNames.release_date}
            from {TableNames.samples}
            where ({ColumnNames.collection_end_date} - {ColumnNames.collection_start_date}) <= :max_span_days
        ) midpoints
    ) lagged
    {group_by_clause}
    {order_by_clause}
    """

    async with get_async_session() as session:
        result = await session.execute(text(query), {'max_span_days': max_span_days})
        rows = result.all()
    return [
        {
            "collection_date_bin": date_bin.format_iso_chunk(row[0], row[1]),
            "lag_q1": row.q1,
            "lag_median": row.median,
            "lag_q3": row.q3
        }
        for row in rows
    ]
