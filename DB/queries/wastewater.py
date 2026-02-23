from typing import List
from datetime import date

from sqlalchemy import select, func, text
from sqlalchemy.orm import contains_eager

from DB.engine import get_async_session
from DB.models import Sample, GeoLocation
from DB.queries.date_count_helpers import DateBinOpt, get_extract_clause, get_group_by_clause
from api.models import LineageAbundanceWithSampleInfo, AverageLineageAbundanceInfo, SampleInfo
from parser.parser import parser

from utils.constants import COLLECTION_DATE

async def get_lineage_abundances_by_sample(
    raw_query: str | None,
) -> List[LineageAbundanceWithSampleInfo]:
    if raw_query is not None:
        parsed_query = parser.parse(raw_query)
        user_where_clause = f'and ({parsed_query})'
    else:
        user_where_clause = ''

    async with get_async_session() as session:
        res = await session.execute(
            text(
                f'''
                select
                    s.accession,
                    gl.admin1_name,
                    s.ww_collected_by,
                    s.ww_site_id,
                    l.lineage_name,
                    sl.abundance,
                    s.ww_viral_load,
                    s.ww_catchment_population,
                    s.collection_start_date
                from samples_lineages sl
                inner join lineages l on l.id = sl.lineage_id
                inner join samples s on s.id = sl.sample_id
                inner join geo_locations gl on gl.id = s.geo_location_id
                {user_where_clause}
                '''
            )
        )

    out_data = list()
    for r in res:
        info = LineageAbundanceWithSampleInfo(
            accession=r[0],
            admin1_name=r[1],
            ww_collected_by=r[2],
            ww_site_id=r[3],
            lineage_name=r[4],
            abundance=r[5],
            ww_viral_load=r[6],
            ww_catchment_population=r[7],
            collection_start_date=r[8],
        )
        out_data.append(info)
    return out_data

async def get_averaged_lineage_abundances_by_location(
    geo_bin: str,
    raw_query: str,
    max_span_days: int,
    lineage_name: str | None = None,
    start_date: date | None = None,
    end_date: date | None = None,
) -> List[AverageLineageAbundanceInfo]:
    user_where_clause = ''
    if raw_query is not None:
        user_where_clause = f'where ({parser.parse(raw_query)})'

    is_wildcard = lineage_name is not None and lineage_name.endswith('*')
    parent_lineage_name = lineage_name.rstrip('*') if is_wildcard else None

    # Filter base_data CTE by date
    date_filter = ''
    params: dict = {}
    if start_date is not None and end_date is not None:
        date_filter = '''and s.collection_start_date >= :start_date and s.collection_start_date <= :end_date'''
        params['start_date'] = start_date
        params['end_date'] = end_date

    extract_clause = get_extract_clause(COLLECTION_DATE, DateBinOpt.week, 7)

    match geo_bin:
        case 'admin1_name':
            group_by_clause = get_group_by_clause(DateBinOpt.week, extra_cols=['week_start', 'week_end', 'admin1_name', 'census_region'])
            lp_join_on = 'and lp.admin1_name = tp.admin1_name and lp.census_region = tp.census_region'

        case 'census_region':
            group_by_clause = get_group_by_clause(DateBinOpt.week, extra_cols=['week_start', 'week_end', 'census_region'])
            lp_join_on = 'and lp.census_region = tp.census_region'

            if 'admin1_name' in user_where_clause:
                raise ValueError('admin1_name cannot be used in the raw_query when geo_bin is "census_region"')
        case _:
            raise ValueError(f'illegal value for geo_bin: {repr(geo_bin)}')
    
    lineage_where = ''
    if lineage_name is not None and not is_wildcard:
        lineage_where = 'where lineage_name = :lineage_name'
        params['lineage_name'] = lineage_name

    if is_wildcard:
        params['parent_lineage_name'] = parent_lineage_name
        query = f'''
            with lineage_filter as (
                select l.id as lineage_id
                from lineages l
                where l.lineage_name = :parent_lineage_name

                union

                select ldc.child_id as lineage_id
                from lineages l
                inner join lineages_deep_children ldc on ldc.parent_id = l.id
                where l.lineage_name = :parent_lineage_name
            ),
            all_base_data as (
                select
                    l.lineage_name,
                    ls.lineage_system_name,
                    gl.admin1_name,
                    s.census_region as census_region,
                    s.collection_start_date,
                    s.collection_end_date,
                    sl.abundance,
                    s.ww_catchment_population,
                    s.ww_viral_load,
                    sl.abundance * s.ww_catchment_population as pop_weighted_prevalence,
                    (
                        s.collection_start_date +
                        ((s.collection_end_date - s.collection_start_date) / 2)
                    )::date as mid_collection_date
                from samples_lineages sl
                inner join lineages l on l.id = sl.lineage_id
                inner join lineage_systems ls on ls.id = l.lineage_system_id
                inner join samples s on s.id = sl.sample_id
                left join geo_locations gl on gl.id = s.geo_location_id
                where (s.collection_end_date - s.collection_start_date) <= {max_span_days}
                {date_filter}
            ),
            lineage_base_data as (
                select
                    gl.admin1_name,
                    s.census_region as census_region,
                    sl.abundance,
                    s.ww_catchment_population,
                    s.ww_viral_load,
                    sl.abundance * s.ww_catchment_population as pop_weighted_prevalence,
                    (
                        s.collection_start_date +
                        ((s.collection_end_date - s.collection_start_date) / 2)
                    )::date as mid_collection_date
                from lineage_filter lf
                inner join samples_lineages sl on sl.lineage_id = lf.lineage_id
                inner join samples s on s.id = sl.sample_id
                left join geo_locations gl on gl.id = s.geo_location_id
                where (s.collection_end_date - s.collection_start_date) <= {max_span_days}
                {date_filter}
            ),
            total_prevalences as (
                select
                    {extract_clause},
                    date_trunc('week', mid_collection_date)::date as week_start,
                    (date_trunc('week', mid_collection_date) + interval '6 days')::date as week_end,
                    admin1_name,
                    census_region,
                    sum(pop_weighted_prevalence) as total_prevalence,
                    count(*) as sample_count,
                    avg(ww_viral_load) as mean_viral_load,
                    avg(ww_catchment_population) as mean_catchment_size
                from all_base_data
                {group_by_clause}
            ),
            lineage_prevalences as (
                select
                    {extract_clause},
                    date_trunc('week', mid_collection_date)::date as week_start,
                    (date_trunc('week', mid_collection_date) + interval '6 days')::date as week_end,
                    admin1_name,
                    census_region,
                    :parent_lineage_name as lineage_name,
                    sum(pop_weighted_prevalence) as lineage_prevalence,
                    count(*) as sample_count
                from lineage_base_data
                {group_by_clause}
            ),
            result_data as (
                select
                    lp.year,
                    lp.chunk,
                    (lp.year || LPAD(lp.chunk::text, 2, '0'))::int as epiweek,
                    lp.week_start,
                    lp.week_end,
                    lp.lineage_name as lineage,
                    lp.census_region,
                    lp.admin1_name,
                    lp.sample_count,
                    tp.mean_viral_load,
                    tp.mean_catchment_size,
                    lp.lineage_prevalence / tp.total_prevalence as mean_lineage_prevalence
                from lineage_prevalences lp
                join total_prevalences tp
                    on lp.year = tp.year
                    and lp.chunk = tp.chunk
                    and lp.week_start = tp.week_start
                    and lp.week_end = tp.week_end
                    {lp_join_on}
            )
            select *
            from result_data
            {user_where_clause};
        '''
    else:
        # Use regular logic without lineage filtering
        query = f'''
            with base_data as (
                select
                    l.lineage_name,
                    ls.lineage_system_name,
                    gl.admin1_name,
                    s.census_region as census_region,
                    s.collection_start_date,
                    s.collection_end_date,
                    sl.abundance,
                    s.ww_catchment_population,
                    s.ww_viral_load,
                    sl.abundance * s.ww_catchment_population as pop_weighted_prevalence,
                    (
                        s.collection_start_date +
                        ((s.collection_end_date - s.collection_start_date) / 2)
                    )::date as mid_collection_date
                from samples_lineages sl
                inner join lineages l on l.id = sl.lineage_id
                inner join lineage_systems ls on ls.id = l.lineage_system_id
                inner join samples s on s.id = sl.sample_id
                left join geo_locations gl on gl.id = s.geo_location_id
                where (s.collection_end_date - s.collection_start_date) <= {max_span_days}
                {date_filter}
            ),
            total_prevalences as (
                select
                    {extract_clause},
                    date_trunc('week', mid_collection_date)::date as week_start,
                    (date_trunc('week', mid_collection_date) + interval '6 days')::date as week_end,
                    admin1_name,
                    census_region,
                    sum(pop_weighted_prevalence) as total_prevalence,
                    count(*) as sample_count,
                    avg(ww_viral_load) as mean_viral_load,
                    avg(ww_catchment_population) as mean_catchment_size
                from base_data
                {group_by_clause}
            ),
            lineage_prevalences as (
                select
                    {extract_clause},
                    date_trunc('week', mid_collection_date)::date as week_start,
                    (date_trunc('week', mid_collection_date) + interval '6 days')::date as week_end,
                    admin1_name,
                    census_region,
                    lineage_name,
                    sum(pop_weighted_prevalence) as lineage_prevalence,
                    count(*) as sample_count
                from base_data
                {lineage_where}
                {group_by_clause + ', lineage_name'}
            ),
            result_data as (
                select
                    lp.year,
                    lp.chunk,
                    (lp.year || LPAD(lp.chunk::text, 2, '0'))::int as epiweek,
                    lp.week_start,
                    lp.week_end,
                    lp.lineage_name as lineage,
                    lp.census_region,
                    lp.admin1_name,
                    lp.sample_count,
                    tp.mean_viral_load,
                    tp.mean_catchment_size,
                    lp.lineage_prevalence / tp.total_prevalence as mean_lineage_prevalence
                from lineage_prevalences lp
                join total_prevalences tp
                    on lp.year = tp.year
                and lp.chunk = tp.chunk
                and lp.week_start = tp.week_start
                and lp.week_end = tp.week_end
                {lp_join_on}
            )
            select *
            from result_data
            {user_where_clause};
        '''
        
    async with get_async_session() as session:
        res = await session.execute(text(query), params if params else {})

    out_data = list()
    if geo_bin == 'admin1_name':
        for r in res:
            info = AverageLineageAbundanceInfo(
                year=r[0],
                chunk=r[1],
                epiweek=r[2],
                week_start=r[3],
                week_end=r[4],  
                lineage_name=r[5],
                census_region=r[6],
                geo_admin1_name=r[7],
                sample_count=r[8],
                mean_viral_load=r[9],
                mean_catchment_size=r[10],
                mean_lineage_prevalence=r[11]
            )
            out_data.append(info)
    elif geo_bin == 'census_region':
        for r in res:
            info = AverageLineageAbundanceInfo(
                year=r[0],
                chunk=r[1],
                epiweek=r[2],
                week_start=r[3],
                week_end=r[4],
                lineage_name=r[5],
                census_region=r[6],
                geo_admin1_name=None,
                sample_count=r[7],
                mean_viral_load=r[8],
                mean_catchment_size=r[9],
                mean_lineage_prevalence=r[10]
            )
            out_data.append(info)
 
    return out_data


async def get_latest_sample(query: str | None) -> List[SampleInfo]:
    # Parse and map field names to actual database columns
    user_defined_query = None
    if query is not None:
        user_defined_query = parser.parse(query)
    
    date_subquery = (
        select(
            func.max(Sample.collection_start_date).label("latest_collection_date")
        )
        .select_from(Sample)
        .join(GeoLocation, Sample.geo_location_id == GeoLocation.id, isouter=True)
    )
    
    if user_defined_query is not None:
        date_subquery = date_subquery.where(text(user_defined_query))
    
    samples_query = (
        select(Sample, GeoLocation)
        .join(GeoLocation, Sample.geo_location_id == GeoLocation.id, isouter=True)
        .options(contains_eager(Sample.r_geo_location))
        .where(Sample.collection_start_date == date_subquery.scalar_subquery())
    )
    
    if user_defined_query is not None:
        samples_query = samples_query.where(text(user_defined_query))
    
    async with get_async_session() as session:
        samples = await session.scalars(samples_query)
        out_data = []
        for s in samples:
            sample_info = SampleInfo.from_db_object(s)
            out_data.append(sample_info)
    return out_data