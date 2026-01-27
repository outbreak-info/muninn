from collections import defaultdict
from typing import List, Dict

from sqlalchemy import select, func, distinct, text, and_
from sqlalchemy.orm import contains_eager

from DB.engine import get_async_session
from DB.models import LineageSystem, Lineage, Sample, SampleLineage, GeoLocation, Allele, Mutation
from DB.queries.date_count_helpers import get_extract_clause, get_group_by_clause, get_order_by_cause, \
    MID_COLLECTION_DATE_CALCULATION
from api.models import LineageCountInfo, LineageAbundanceInfo, LineageInfo, LineageAbundanceSummaryInfo, \
    MutationProfileInfo, AverageLineageAbundanceInfo, LineageAbundanceWithSampleInfo
from parser.parser import parser
from utils.constants import DateBinOpt, NtOrAa, NUCLEOTIDE_CHARACTERS, TableNames, StandardColumnNames, COLLECTION_DATE


async def get_all_lineages_by_lineage_system(lineage_system_name: str) -> List[LineageInfo]:
    async with get_async_session() as session:
        res = await session.execute(
            select(
                Lineage.id.label("lineage_id"),
                Lineage.lineage_name.label("lineage_name"),
                LineageSystem.id.label("lineage_system_id"),
                LineageSystem.lineage_system_name.label("lineage_system_name"),
            )
            .join(LineageSystem, Lineage.lineage_system_id == LineageSystem.id)
            .where(LineageSystem.lineage_system_name == lineage_system_name)
        )
        out_data = [LineageInfo(**row) for row in res.mappings().all()]
    return out_data


async def get_sample_counts_by_lineage(samples_raw_query: str | None) -> List[LineageCountInfo]:
    lineage_count_query = (
        select(SampleLineage, Lineage, LineageSystem)
        .join(Lineage, Lineage.id == SampleLineage.lineage_id, isouter=True)
        .join(LineageSystem, LineageSystem.id == Lineage.lineage_system_id, isouter=True)
        .with_only_columns(
            LineageSystem.lineage_system_name,
            Lineage.lineage_name,
            func.count(distinct(SampleLineage.sample_id)).label('count1')
        )
        .group_by(LineageSystem.lineage_system_name, Lineage.lineage_name)
        .order_by(text('count1 desc'))
    )

    if samples_raw_query is not None:
        user_query = parser.parse(samples_raw_query)
        lineage_count_query = lineage_count_query.where(
            SampleLineage.sample_id.in_(
                select(Sample.id)
                .join(GeoLocation, GeoLocation.id == Sample.geo_location_id, isouter=True)
                .where(text(user_query))
            )
        )

    async with get_async_session() as session:
        res = await session.execute(lineage_count_query)
        out_data = []
        for r in res:
            out_data.append(
                LineageCountInfo(
                    count=r[2],
                    lineage=r[1],
                    lineage_system=r[0]
                )
            )
    return out_data


async def get_abundances(raw_query: str | None) -> List[LineageAbundanceInfo]:
    lineage_query = (
        select(SampleLineage, Lineage, LineageSystem)
        .join(Lineage, Lineage.id == SampleLineage.lineage_id, isouter=True)
        .options(contains_eager(SampleLineage.r_lineage))
        .join(LineageSystem, LineageSystem.id == Lineage.lineage_system_id, isouter=True)
        .options(contains_eager(Lineage.r_lineage_system))
        .join(Sample, Sample.id == SampleLineage.sample_id, isouter=True)
        .options(contains_eager(SampleLineage.r_sample))
        .join(GeoLocation, GeoLocation.id == Sample.geo_location_id, isouter=True)
    )

    if raw_query is not None:
        user_query = parser.parse(raw_query)
        lineage_query = lineage_query.where(
            and_(
                SampleLineage.is_consensus_call == False,
                text(user_query)
            )
        )
    else:
        lineage_query = lineage_query.where(
            SampleLineage.is_consensus_call == False
        )

    out_data = []
    async with get_async_session() as session:
        res: List[SampleLineage] = await session.scalars(lineage_query)
        for samp_lin in res:
            out_data.append(
                LineageAbundanceInfo(
                    lineage_info=LineageInfo(
                        lineage_id=samp_lin.lineage_id,
                        lineage_name=samp_lin.r_lineage.lineage_name,
                        lineage_system_name=samp_lin.r_lineage.r_lineage_system.lineage_system_name,
                        lineage_system_id=samp_lin.r_lineage.lineage_system_id,
                    ),
                    sample_id=samp_lin.sample_id,
                    accession=samp_lin.r_sample.accession,
                    abundance=samp_lin.abundance
                )
            )
    return out_data


async def get_abundance_summaries(raw_query: str | None) -> List[LineageAbundanceSummaryInfo]:
    lineage_query = (
        select(SampleLineage, Lineage, LineageSystem)
        .join(Lineage, Lineage.id == SampleLineage.lineage_id, isouter=True)
        .options(contains_eager(SampleLineage.r_lineage))
        .join(LineageSystem, LineageSystem.id == Lineage.lineage_system_id, isouter=True)
        .options(contains_eager(Lineage.r_lineage_system))
        .join(Sample, Sample.id == SampleLineage.sample_id, isouter=True)
        .join(GeoLocation, GeoLocation.id == Sample.geo_location_id, isouter=True)
        .with_only_columns(
            Lineage.lineage_name,
            LineageSystem.lineage_system_name,
            func.count().label('samp_count'),
            func.min(SampleLineage.abundance).label('min'),
            func.percentile_cont(0.25).within_group(SampleLineage.abundance).label('q1'),
            func.percentile_cont(0.5).within_group(SampleLineage.abundance).label('median'),
            func.percentile_cont(0.75).within_group(SampleLineage.abundance).label('q3'),
            func.max(SampleLineage.abundance).label('max'),
        )
        .group_by(Lineage.lineage_name, LineageSystem.lineage_system_name)
    )

    if raw_query is not None:
        user_query = parser.parse(raw_query)
        lineage_query = lineage_query.where(
            and_(
                SampleLineage.is_consensus_call == False,
                text(user_query)
            )
        )
    else:
        lineage_query = lineage_query.where(
            SampleLineage.is_consensus_call == False
        )

    out_data = []
    async with get_async_session() as session:
        res = await session.execute(lineage_query)
        for r in res:
            out_data.append(
                LineageAbundanceSummaryInfo(
                    lineage_name=r[0],
                    lineage_system_name=r[1],
                    sample_count=r[2],
                    abundance_min=r[3],
                    abundance_q1=r[4],
                    abundance_median=r[5],
                    abundance_q3=r[6],
                    abundance_max=r[7]
                )
            )
    return out_data


async def get_abundance_summaries_by_simple_date(
    group_by: str,
    raw_query: str,
    date_bin: DateBinOpt,
    days: int,
) -> Dict[str, List[LineageAbundanceSummaryInfo]]:
    user_where_clause = ''
    if raw_query is not None:
        user_where_clause = f'and ({parser.parse(raw_query)})'

    extract_clause = get_extract_clause(group_by, date_bin, days)
    group_by_clause = get_group_by_clause(
        date_bin,
        [StandardColumnNames.lineage_name, StandardColumnNames.lineage_system_name]
    )
    order_by_clause = get_order_by_cause(date_bin)

    async with get_async_session() as session:
        res = await session.execute(
            text(
                f'''
                select 
                {extract_clause},
                l.lineage_name,
                ls.lineage_system_name,
                count(*) as samp_count,
                min(sl.abundance) as min,
                percentile_cont(0.25) within group (order by sl.abundance) as q1,
                percentile_cont(0.5) within group (order by sl.abundance) as median,
                percentile_cont(0.75) within group (order by sl.abundance) as q3,
                max(sl.abundance) as max
                from samples_lineages sl
                inner join lineages l on l.id = sl.lineage_id 
                inner join lineage_systems ls on ls.id = l.lineage_system_id 
                inner join samples s on s.id = sl.sample_id 
                left join geo_locations gl on gl.id = s.geo_location_id 
                where sl.is_consensus_call = false {user_where_clause} 
                {group_by_clause}
                {order_by_clause}
                '''
            )
        )

    out_data = dict()
    for r in res:
        date = date_bin.format_iso_chunk(r[0], r[1])
        info = LineageAbundanceSummaryInfo(
            lineage_name=r[2],
            lineage_system_name=r[3],
            sample_count=r[4],
            abundance_min=r[5],
            abundance_q1=r[6],
            abundance_median=r[7],
            abundance_q3=r[8],
            abundance_max=r[9]
        )
        try:
            out_data[date].append(info)
        except KeyError:
            out_data[date] = [info]
    return out_data

    # wastewater-specific
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

# wastewater-specific
async def get_averaged_abundances_by_location(
    date_bin: DateBinOpt,
    geo_bin: str,
    raw_query: str,
) -> List[AverageLineageAbundanceInfo]:
    user_where_clause = ''
    if raw_query is not None:
        user_where_clause = f'where ({parser.parse(raw_query)})' \
            .replace('state', 'admin1_name')

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
            if user_where_clause.contains('state'):
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
                state=r[7],
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

async def get_abundance_summaries_by_collection_date(
    date_bin: DateBinOpt,
    days: int,
    raw_query: str | None,
    max_span_days: int,
) -> Dict[str, List[LineageAbundanceSummaryInfo]]:
    user_where_clause = ''
    if raw_query is not None:
        user_where_clause = f'and ({parser.parse(raw_query)})'

    extract_clause = get_extract_clause(COLLECTION_DATE, date_bin, days)
    group_by_clause = get_group_by_clause(
        date_bin,
        [StandardColumnNames.lineage_name, StandardColumnNames.lineage_system_name]
    )
    order_by_clause = get_order_by_cause(date_bin)

    async with get_async_session() as session:
        res = await session.execute(
            text(
                f'''
                select 
                {extract_clause},
                lineage_name,
                lineage_system_name,
                count(*) as samp_count,
                min(abundance) as min,
                percentile_cont(0.25) within group (order by abundance) as q1,
                percentile_cont(0.5) within group (order by abundance) as median,
                percentile_cont(0.75) within group (order by abundance) as q3,
                max(abundance) as max 
                from(
                    select
                    *,
                    {MID_COLLECTION_DATE_CALCULATION}
                    from(
                        select 
                        l.lineage_name,
                        ls.lineage_system_name,
                        sl.abundance,
                        collection_start_date,
                        collection_end_date,
                        collection_end_date - collection_start_date as collection_span
                        from samples_lineages sl
                        inner join lineages l on l.id = sl.lineage_id 
                        inner join lineage_systems ls on ls.id = l.lineage_system_id 
                        inner join samples s on s.id = sl.sample_id 
                        left join geo_locations gl on gl.id = s.geo_location_id 
                        where sl.is_consensus_call = false {user_where_clause}
                        
                    )
                    where collection_span <= {max_span_days}
                )
                {group_by_clause}
                {order_by_clause}
                '''
            )
        )
    out_data = dict()
    for r in res:
        date = date_bin.format_iso_chunk(r[0], r[1])
        info = LineageAbundanceSummaryInfo(
            lineage_name=r[2],
            lineage_system_name=r[3],
            sample_count=r[4],
            abundance_min=r[5],
            abundance_q1=r[6],
            abundance_median=r[7],
            abundance_q3=r[8],
            abundance_max=r[9]
        )
        try:
            out_data[date].append(info)
        except KeyError:
            out_data[date] = [info]
    return out_data


async def get_mutation_incidence(
    lineage: str,
    lineage_system_name: str,
    change_bin: NtOrAa,
    prevalence_threshold: float,
    match_reference: bool,
    raw_query: str | None
):
    user_where_clause = ''
    if raw_query is not None:
        user_where_clause = f'and ({parser.parse(raw_query)})'

    async with get_async_session() as session:
        sample_count = await session.scalar(
            text(
                f'''
                select count(*)
                from samples s
                left join samples_lineages sl on sl.sample_id = s.id
                left join lineages l on l.id = sl.lineage_id
                left join lineage_systems ls on ls.id = l.lineage_system_id 
                WHERE l.lineage_name = :input_lineage and ls.lineage_system_name = :input_lineage_system_name
                {user_where_clause}
                '''
            ), {
                'input_lineage': lineage,
                'input_lineage_system_name': lineage_system_name
            }
        )
        sample_count = float(sample_count)

        sample_subset_query = f"""
        select s.id from samples s
        inner join samples_lineages sl ON sl.sample_id = s.id
        inner join lineages l on l.id = sl.lineage_id
        inner join lineage_systems ls on ls.id = l.lineage_system_id
        where l.lineage_name = '{lineage}' and ls.lineage_system_name='{lineage_system_name}'
        {user_where_clause}
        """

        if change_bin == NtOrAa.nt:
            not_reference = 'where ref_nt <> alt_nt'
            if match_reference:
                not_reference = ''

            res = await session.execute(
                text(
                    f'''
                    WITH sample_subset as (
                        {sample_subset_query}
                    ) SELECT ref_nt, position_nt, alt_nt, region, count(*) as mutation_count, count(*) / {sample_count} as mutation_prevalence from sample_subset
                    inner join mutations m ON m.sample_id = sample_subset.id
                    inner join alleles a on a.id = m.allele_id
                    {not_reference}
                    group by ref_nt, position_nt, alt_nt, region
                    having count(*) / {sample_count} >= {prevalence_threshold};
                    '''
                )
            )
        else:
            not_reference = 'where ref_aa <> alt_aa'
            if match_reference:
                not_reference = ''
            res = await session.execute(
                text(
                    f'''
                    WITH sample_subset as (
                        {sample_subset_query}
                    ),
                    sample_aa AS (
                    SELECT DISTINCT m.sample_id,
                                    t.amino_acid_id
                    FROM   mutations    m
                    JOIN   {TableNames.mutations_translations} t ON t.{StandardColumnNames.mutation_id} = m.id
                    ) SELECT ref_aa, position_aa, alt_aa, gff_feature, count(*) as mutation_count, count(*) / {sample_count} as mutation_prevalence
                    from sample_subset
                    inner join sample_aa ON sample_aa.sample_id = sample_subset.id
                    inner join amino_acids aa on aa.id = sample_aa.amino_acid_id
                    {not_reference}
                    group by ref_aa, position_aa, alt_aa, gff_feature
                    having count(*) / {sample_count} >= {prevalence_threshold};
                    '''
                )
            )

    out = defaultdict(list)
    for ref, pos, alt, region_or_gff, count, prevalence in res:
        out[region_or_gff].append({"ref": ref, "alt": alt, "pos": pos, "count": count, "prevalence": prevalence})
    return {'sample_count': sample_count, 'mutation_counts': out}


async def get_mutation_profile(lineage: str, lineage_system_name: str, samples_raw_query: str | None) -> List[
    'MutationProfileInfo']:
    samples_query = parser.parse(samples_raw_query) if samples_raw_query else None
    query = (
        select(
            Allele.region,
            Allele.ref_nt,
            Allele.alt_nt,
            func.count().label("count")
        )
        .select_from(Mutation)
        .join(Sample, Sample.id == Mutation.sample_id)
        .join(Allele, Allele.id == Mutation.allele_id)
        .join(SampleLineage, Sample.id == SampleLineage.sample_id)
        .join(Lineage, SampleLineage.lineage_id == Lineage.id)
        .join(LineageSystem, Lineage.lineage_system_id == LineageSystem.id)
        .where(
            LineageSystem.lineage_system_name == lineage_system_name,
            Lineage.lineage_name == lineage,
            Allele.alt_nt.in_(NUCLEOTIDE_CHARACTERS),
            Allele.ref_nt.in_(NUCLEOTIDE_CHARACTERS)
        )
        .group_by(
            Allele.ref_nt,
            Allele.alt_nt,
            Allele.region,
            Lineage.lineage_name
        )
    )
    if samples_query is not None:
        query = (
            query.where(text(samples_query))
        )
    async with get_async_session() as session:
        results = await session.execute(query)
        out_data = [MutationProfileInfo(**row) for row in results.mappings().all()]
    return out_data
