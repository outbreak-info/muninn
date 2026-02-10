from typing import List

from sqlalchemy import select, func, text
from sqlalchemy.orm import contains_eager

from DB.engine import get_async_session
from DB.models import Sample, GeoLocation
from DB.queries.date_count_helpers import DateBinOpt
from api.models import LineageAbundanceWithSampleInfo, AverageLineageAbundanceInfo, SampleInfo
from parser.parser import parser

async def get_lineage_abundances_by_metadata(
    raw_query: str | None,
) -> List[LineageAbundanceWithSampleInfo]:
    if raw_query is not None:
        parsed_query = parser.parse(raw_query)
        # Replace field references to use proper table aliases
        parsed_query = parsed_query \
            .replace("collection_start_date", "s.collection_start_date") \
            .replace("collection_end_date", "s.collection_end_date") \
            .replace("ww_site_id", "s.ww_site_id") \
            .replace("ww_collected_by", "s.ww_collected_by") \
            .replace("ww_viral_load", "s.ww_viral_load") \
            .replace("ww_catchment_population", "s.ww_catchment_population")
        user_where_clause = f'and ({parsed_query})'

    async with get_async_session() as session:
        res = await session.execute(
            text(
                f'''
                select
                    s.accession,
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
                {user_where_clause}
                '''
            )
        )

    out_data = list()
    for r in res:
        info = LineageAbundanceWithSampleInfo(
            accession=r[0],
            ww_collected_by=r[1],
            ww_site_id=r[2],
            lineage_name=r[3],
            abundance=r[4],
            ww_viral_load=r[5],
            ww_catchment_population=r[6],
            collection_start_date=r[7]
        )
        out_data.append(info)
    return out_data

async def get_averaged_parent_abundances_by_location(
    parent_lineage_name: str,
    date_bin: DateBinOpt,
    geo_bin: str,
    raw_query: str,
) -> List[AverageLineageAbundanceInfo]:
    user_where_clause = ''
    if raw_query is not None:
        user_where_clause = f'where ({parser.parse(raw_query)})'

    match date_bin:
        case DateBinOpt.week | DateBinOpt.month:
            extract_clause = f'''
                extract(year from mid_collection_date) as year,
                extract({date_bin} from mid_collection_date) as chunk
               '''
            group_by_date_cols = 'year, chunk'
    
        case _:
            raise ValueError(f'illegal value for date_bin: {repr(date_bin)}')

    match geo_bin:
        case 'state':
            group_by_geo_level = 'state, census_region'
            lp_group_by = 'lp.state, lp.census_region'

        case 'census_region':
            group_by_geo_level =  'census_region'
            lp_group_by = 'lp.census_region'
        case _:
            raise ValueError(f'illegal value for geo_bin: {repr(geo_bin)}')

    async with get_async_session() as session:
        res = await session.execute(
            text(
                f'''
                with lineage_filter as (
                    select l.id as lineage_id
                    from lineages l
                    where l.lineage_name = '{parent_lineage_name}'

                    union

                    select ldc.child_id as lineage_id
                    from lineages l
                    inner join lineages_deep_children ldc on ldc.parent_id = l.id
                    where l.lineage_name = '{parent_lineage_name}'
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
                    where (s.collection_end_date - s.collection_start_date) <= 30
                ),
                filtered_base_data as (
                    select *
                    from all_base_data
                    where lineage_name in (
                        select l.lineage_name
                        from lineages l
                        where l.id in (select lineage_id from lineage_filter)
                    )
                ),
                total_prevalences as (
                    select
                        extract(year from mid_collection_date) as year,
                        extract(week from mid_collection_date) as week,
                        date_trunc('week', mid_collection_date)::date as week_start,
                        (date_trunc('week', mid_collection_date) + interval '6 days')::date as week_end,
                        admin1_name,
                        census_region,
                        sum(pop_weighted_prevalence) as total_prevalence,
                        count(*) as sample_count,
                        avg(ww_viral_load) as mean_viral_load,
                        avg(ww_catchment_population) as mean_catchment_size
                    from all_base_data
                    group by year, week, week_start, week_end, admin1_name, census_region
                ),
                lineage_prevalences as (
                    select
                        extract(year from mid_collection_date) as year,
                        extract(week from mid_collection_date) as week,
                        date_trunc('week', mid_collection_date)::date as week_start,
                        (date_trunc('week', mid_collection_date) + interval '6 days')::date as week_end,
                        admin1_name,
                        census_region,
                        '{parent_lineage_name}' as lineage_name,
                        sum(pop_weighted_prevalence) as lineage_prevalence,
                        count(*) as sample_count
                    from filtered_base_data
                    group by year, week, week_start, week_end, admin1_name, census_region
                ),
                result_data as (
                    select
                        lp.year,
                        lp.week,
                        (lp.year || LPAD(lp.week::text, 2, '0'))::int as epiweek,
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
                        and lp.week = tp.week
                        and lp.week_start = tp.week_start
                        and lp.week_end = tp.week_end
                        and lp.admin1_name = tp.admin1_name
                        and lp.census_region = tp.census_region
                )
                select *
                from result_data
                {user_where_clause};
                '''
            )
        )

    out_data = list()
    if geo_bin == 'state':
        for r in res:
            info = AverageLineageAbundanceInfo(
                year=r[0],
                week=r[1],
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
    # elif geo_bin == 'census_region':
    #     for r in res:
    #         info = AverageLineageAbundanceInfo(
    #             year=r[0],
    #             chunk=r[1],
    #             epiweek=r[2],
    #             week_start=r[3],
    #             week_end=r[4],
    #             lineage_name=r[5],
    #             state=None,
    #             census_region=r[6],
    #             sample_count=r[7],
    #             mean_lineage_prevalence=r[8]
    #         )
    #         out_data.append(info)
    return out_data

async def get_averaged_lineage_abundances_by_location(
    date_bin: DateBinOpt,
    geo_bin: str,
    raw_query: str,
) -> List[AverageLineageAbundanceInfo]:
    user_where_clause = ''
    if raw_query is not None:
        user_where_clause = f'where ({parser.parse(raw_query)})'

    match date_bin:
        case DateBinOpt.week | DateBinOpt.month:
            extract_clause = f'''
                extract(year from mid_collection_date) as year,
                extract({date_bin} from mid_collection_date) as chunk
               '''
            group_by_date_cols = 'year, chunk'
    
        case _:
            raise ValueError(f'illegal value for date_bin: {repr(date_bin)}')
        

    match geo_bin:
        case 'state':
            group_by_geo_level = 'admin1_name, census_region'
            lp_group_by = 'lp.admin1_name, lp.census_region'

        case 'census_region':
            group_by_geo_level =  'census_region'
            lp_group_by = 'lp.census_region'
            if user_where_clause.contains('admin1_name'):
                raise ValueError('state cannot be used in the raw_query when geo_bin is "census_region"')
        case _:
            raise ValueError(f'illegal value for geo_bin: {repr(geo_bin)}')
        
    async with get_async_session() as session:
        res = await session.execute(
            text(
                f'''
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
                    where (s.collection_end_date - s.collection_start_date) <= 30
                ),
                total_prevalences as (
                    select
                        extract(year from mid_collection_date) as year,
                        extract(week from mid_collection_date) as week,
                        date_trunc('week', mid_collection_date)::date as week_start,
                        (date_trunc('week', mid_collection_date) + interval '6 days')::date as week_end,
                        admin1_name,
                        census_region,
                        sum(pop_weighted_prevalence) as total_prevalence,
                        count(*) as sample_count,
                        avg(ww_viral_load) as mean_viral_load,
                        avg(ww_catchment_population) as mean_catchment_size
                    from base_data
                    group by year, week, week_start, week_end, admin1_name, census_region
                ),
                lineage_prevalences as (
                    select
                        extract(year from mid_collection_date) as year,
                        extract(week from mid_collection_date) as week,
                        date_trunc('week', mid_collection_date)::date as week_start,
                        (date_trunc('week', mid_collection_date) + interval '6 days')::date as week_end,
                        admin1_name,
                        census_region,
                        lineage_name,
                        sum(pop_weighted_prevalence) as lineage_prevalence,
                        count(*) as sample_count
                    from base_data
                    group by year, week, week_start, week_end, admin1_name, census_region, lineage_name
                ),
                result_data as (
                    select
                        lp.year,
                        lp.week,
                        (lp.year || LPAD(lp.week::text, 2, '0'))::int as epiweek,
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
                    and lp.week = tp.week
                    and lp.week_start = tp.week_start
                    and lp.week_end = tp.week_end
                    and lp.admin1_name = tp.admin1_name
                    and lp.census_region = tp.census_region
                )
                select *
                from result_data
                {user_where_clause};
                '''
            )
        )

    out_data = list()
    if geo_bin == 'state':
        for r in res:
            info = AverageLineageAbundanceInfo(
                year=r[0],
                week=r[1],
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
    # elif geo_bin == 'census_region':
    #     for r in res:
    #         info = AverageLineageAbundanceInfo(
    #             year=r[0],
    #             chunk=r[1],
    #             epiweek=r[2],
    #             week_start=r[3],
    #             week_end=r[4],
    #             lineage_name=r[5],
    #             state=None,
    #             census_region=r[6],
    #             sample_count=r[7],
    #             mean_lineage_prevalence=r[8]
    #         )
    #         out_data.append(info)
 
    return out_data


async def get_latest_sample(query: str | None) -> List[SampleInfo]:
    # Parse and map field names to actual database columns
    user_defined_query = None
    if query is not None:
        user_defined_query = parser.parse(query) \
            .replace('admin1_name', 'geo_locations.admin1_name')
    
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
            epiweek = None
            if s.collection_start_date:
                year = s.collection_start_date.isocalendar()[0]
                week = s.collection_start_date.isocalendar()[1]
                epiweek = int(f"{year}{week:02d}")
            
            sample_info = SampleInfo.from_db_object(s).model_copy(update={"epiweek": epiweek})
            out_data.append(sample_info)
    return out_data