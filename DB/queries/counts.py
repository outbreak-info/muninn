import datetime
from typing import Type, List, Any, Dict

from sqlalchemy import text, select, Result
from sqlalchemy.sql.functions import func

from DB.engine import get_async_session
from DB.models import Sample, GeoLocation, IntraHostVariant, AminoAcid, Allele, Mutation, Translation
from parser.parser import parser
from utils.constants import DateBinOpt, NtOrAa


async def count_samples_by_column(by_col: str):
    async with get_async_session() as session:
        res = await session.execute(
            select(Sample, GeoLocation)
            .join(GeoLocation, GeoLocation.id == Sample.geo_location_id, isouter=True)
            .select_from(Sample)
            .with_only_columns(text(by_col), func.count().label('count1'))
            .group_by(text(by_col))
            .order_by(text('count1 desc'))
        )
        return await _package_count_by_column(res)


async def count_variants_by_column(by_col: str):
    async with get_async_session() as session:
        res = await session.execute(
            select(IntraHostVariant, Allele, Translation, AminoAcid)
            .join(Allele, Allele.id == IntraHostVariant.allele_id, isouter=True)
            .join(Translation, Translation.id == IntraHostVariant.translation_id, isouter=True)
            .join(AminoAcid, AminoAcid.id == Translation.amino_acid_id, isouter=True)
            .with_only_columns(text(by_col), func.count().label('count1'))
            .group_by(text(by_col))
            .order_by(text('count1 desc'))
        )
        return await _package_count_by_column(res)


async def count_mutations_by_column(by_col: str):
    async with get_async_session() as session:
        res = await session.execute(
            select(Mutation, Allele, Translation, AminoAcid)
            .join(Allele, Allele.id == Mutation.allele_id, isouter=True)
            .join(Translation, Translation.id == Mutation.translation_id, isouter=True)
            .join(AminoAcid, AminoAcid.id == Translation.amino_acid_id, isouter=True)
            .with_only_columns(text(by_col), func.count().label('count1'))
            .group_by(text(by_col))
            .order_by(text('count1 desc'))
        )
        return await _package_count_by_column(res)


async def _package_count_by_column(query_result: Result[tuple[Any, int]] | List[tuple]) -> Dict[str, int]:
    return {str(r[0]): r[1] for r in query_result}


async def count_samples_by_simple_date(
    group_by: str,
    date_bin: DateBinOpt,
    days: int | None,
    raw_query: str | None
):
    where_clause = ''
    if raw_query is not None:
        where_clause = f'where {parser.parse(raw_query)}'

    match date_bin:
        case DateBinOpt.week | DateBinOpt.month:
            extract_clause = f'''
            extract(year from {group_by}) as year,
            extract({date_bin} from {group_by}) as chunk
            '''
            group_by_date_cols = 'year, chunk'

        case DateBinOpt.day:
            origin = datetime.date.today()
            extract_clause = f'''
            date_bin('{days} days', {group_by}, '{origin}') + interval '{days} days' as bin_end,
            date_bin('{days} days', {group_by}, '{origin}') as bin_start
            '''
            group_by_date_cols = 'bin_start'

        case _:
            raise ValueError(f'illegal value for date_bin: {repr(date_bin)}')

    async with get_async_session() as session:
        res = await session.execute(
            text(
                f'''
                select 
                {extract_clause}, 
                count(*)
                from samples s
                left join geo_locations gl on gl.id = s.geo_location_id
                {where_clause}
                group by {group_by_date_cols} 
                order by {group_by_date_cols}
                '''
            )
        )

    out_data = dict()
    for r in res:
        date = date_bin.format_iso_chunk(r[0], r[1])
        out_data[date] = r[2]
    return out_data


async def count_samples_by_collection_date(
    date_bin: DateBinOpt,
    days: int,
    raw_query: str | None,
    max_span_days: int,
) -> Dict[str, int]:
    where_clause = ''
    if raw_query is not None:
        where_clause = f'where {parser.parse(raw_query)}'

    match date_bin:
        case DateBinOpt.week | DateBinOpt.month:
            extract_clause = f'''
            extract(year from mid_collection_date) as year,  
            extract({date_bin} from mid_collection_date) as chunk
            '''

            group_and_order_clause = f'''
            group by year, chunk
            order by year, chunk
            '''

        case DateBinOpt.day:
            origin = datetime.date.today()
            extract_clause = f'''
            date_bin('{days} days', mid_collection_date, '{origin}') + interval '{days} days' as bin_end,
            date_bin('{days} days', mid_collection_date, '{origin}') as bin_start
            '''

            group_and_order_clause = f'''
            group by bin_start, bin_end
            order by bin_start
            '''

        case _:
            raise NotImplementedError

    async with get_async_session() as session:
        res = await session.execute(
            text(
                f'''
                select 
                    {extract_clause},
                    count(*)
                from (
                    select 
                    (collection_start_date + ((collection_end_date - collection_start_date) / 2))::date AS mid_collection_date
                    from (
                        select
                        *,
                        collection_end_date - collection_start_date as collection_span
                        from samples s
                        left join geo_locations gl on gl.id = s.geo_location_id
                        {where_clause}
                    )
                    where collection_span <= {max_span_days}
                )
               {group_and_order_clause}
                '''
            )
        )
    out_data = dict()
    for r in res:
        date = date_bin.format_iso_chunk(r[0], r[1])
        out_data[date] = r[2]
    return out_data


async def count_variants_by_simple_date(
    group_by: str,
    date_bin: DateBinOpt,
    days: int,
    raw_query: str | None,
    change_bin: NtOrAa
):
    return await _count_variants_or_mutations_by_simple_date_bin(
        group_by,
        date_bin,
        days,
        raw_query,
        change_bin,
        IntraHostVariant
    )


async def count_mutations_by_simple_date(
    group_by: str,
    date_bin: DateBinOpt,
    days: int,
    raw_query: str | None,
    change_bin: NtOrAa
):
    return await _count_variants_or_mutations_by_simple_date_bin(
        group_by,
        date_bin,
        days,
        raw_query,
        change_bin,
        Mutation
    )


async def _count_variants_or_mutations_by_simple_date_bin(
    group_by: str,
    date_bin: DateBinOpt,
    days: int,
    raw_query: str | None,
    change_bin: NtOrAa,
    table: Type['IntraHostVariant'] | Type['Mutation']
):
    where_clause = ''
    if raw_query is not None:
        where_clause = f'where {parser.parse(raw_query)}'

    change_fields = f'ref_{change_bin}, position_{change_bin}, alt_{change_bin}'

    match date_bin:
        case DateBinOpt.week | DateBinOpt.month:
            extract_clause = f'''
                   extract(year from {group_by}) as year,
                   extract({date_bin} from {group_by}) as chunk
                   '''
            group_by_date_cols = 'year, chunk'

        case DateBinOpt.day:
            origin = datetime.date.today()
            extract_clause = f'''
                   date_bin('{days} days', {group_by}, '{origin}') + interval '{days} days' as bin_end,
                   date_bin('{days} days', {group_by}, '{origin}') as bin_start
                   '''
            group_by_date_cols = 'bin_start'

        case _:
            raise ValueError(f'illegal value for date_bin: {repr(date_bin)}')

    async with get_async_session() as session:
        res = await session.execute(
            text(
                f'''
                select 
                {extract_clause},
                count(*),
                region,
                {change_fields}
                from (
                    select region, {change_fields}, {group_by} 
                    from samples s
                    inner join {table.__tablename__} VM on VM.sample_id = s.id
                    inner join alleles a on a.id = VM.allele_id
                    left join translations t on t.allele_id = a.id
                    left join amino_acid_substitutions aas on aas.id = t.amino_acid_substitution_id
                    {where_clause}
                )
                group by region, {change_fields}, {group_by_date_cols}
                '''
            )
        )

    out_data = dict()
    for r in res:
        date = date_bin.format_iso_chunk(r[0], r[1])
        count = r[2]
        region = r[3]
        ref = r[4]
        pos = r[5]
        alt = r[6]
        change_name = f'{region}:{ref}{pos}{alt}'

        try:
            out_data[date][change_name] = count
        except KeyError:
            out_data[date] = {change_name: count}
    return out_data


async def count_variants_by_collection_date(
    date_bin: DateBinOpt,
    change_bin: NtOrAa,
    days: int,
    max_span_days: int,
    raw_query: str
):
    return await _count_variants_or_mutations_by_collection_date(
        date_bin,
        change_bin,
        days,
        max_span_days,
        raw_query,
        IntraHostVariant
    )


async def count_mutations_by_collection_date(
    date_bin: DateBinOpt,
    change_bin: NtOrAa,
    days: int,
    max_span_days: int,
    raw_query: str
):
    return await _count_variants_or_mutations_by_collection_date(
        date_bin,
        change_bin,
        days,
        max_span_days,
        raw_query,
        Mutation
    )


async def _count_variants_or_mutations_by_collection_date(
    date_bin: DateBinOpt,
    change_bin: NtOrAa,
    days: int,
    max_span_days: int,
    raw_query: str,
    table: Type[IntraHostVariant] | Type[Mutation]
):
    change_fields = f'ref_{change_bin}, position_{change_bin}, alt_{change_bin}'

    user_where_clause = ''
    if raw_query is not None:
        user_where_clause = f'and ({parser.parse(raw_query)})'

    match date_bin:
        case DateBinOpt.week | DateBinOpt.month:
            extract_clause = f'''
            extract(year from mid_collection_date) as year,
            extract({date_bin} from mid_collection_date) as chunk
            '''

            group_and_order_clause = f'''
            group by year, chunk, gff_feature, {change_fields}
            order by year, chunk
            '''
        case DateBinOpt.day:
            origin = datetime.date.today()
            extract_clause = f'''
            date_bin('{days} days', mid_collection_date, '{origin}') + interval '{days} days' as bin_end,
            date_bin('{days} days', mid_collection_date, '{origin}') as bin_start
            '''

            group_and_order_clause = f'''
            group by bin_start, bin_end, gff_feature, {change_fields}
            order by bin_start
            '''
        case _:
            raise ValueError

    async with get_async_session() as session:
        res = await session.execute(
            text(
                f'''
                select
                {extract_clause},
                count(*),
                gff_feature, {change_fields}
                from(
                    select 
                    *,
                    (collection_start_date + ((collection_end_date - collection_start_date) / 2))::date AS mid_collection_date
                    from (
                        select 
                        gff_feature, {change_fields},
                        collection_start_date, collection_end_date,
                        collection_end_date - collection_start_date as collection_span
                        from samples s
                        left join geo_locations gl on gl.id = s.geo_location_id
                        inner join {table.__tablename__} VM on VM.sample_id = s.id
                        left join translations t on t.id = VM.translation_id
                        left join amino_acids aas on aas.id = t.amino_acid_id
                        left join samples_lineages sl on sl.sample_id = s.id
                        left join lineages l on l.id = sl.lineage_id
                        left join lineage_systems ls on ls.id = l.lineage_system_id
                        where num_nulls(collection_end_date, collection_start_date) = 0 {user_where_clause}
                    )
                    where collection_span <= {max_span_days}
                )
                {group_and_order_clause}
                '''
            )
        )
    out_data = dict()
    for r in res:
        date = date_bin.format_iso_chunk(r[0], r[1])
        count = r[2]
        region = r[3]
        ref = r[4]
        pos = r[5]
        alt = r[6]
        change_name = f'{region}:{ref}{pos}{alt}'

        try:
            out_data[date][change_name] = count
        except KeyError:
            out_data[date] = {change_name: count}
    return out_data


async def count_lineages_by_simple_date(
    group_by: str,
    date_bin: DateBinOpt,
    raw_query: str,
    days: int
) -> Dict[str, Dict[str, Dict[str, int]]]:
    where_clause = ''
    if raw_query is not None:
        where_clause = f'where {parser.parse(raw_query)}'

    match date_bin:
        case DateBinOpt.week | DateBinOpt.month:
            extract_clause = f'''
                extract(year from {group_by}) as year,
                extract({date_bin} from {group_by}) as chunk
                '''

            group_and_order_clause = f'''
                group by year, chunk, lineage_name, lineage_system_name
                order by year, chunk
                '''
        case DateBinOpt.day:
            origin = datetime.date.today()
            extract_clause = f'''
                date_bin('{days} days', {group_by}, '{origin}') + interval '{days} days' as bin_end,
                date_bin('{days} days', {group_by}, '{origin}') as bin_start
                '''

            group_and_order_clause = f'''
                group by bin_start, bin_end, lineage_name, lineage_system_name
                order by bin_start
                '''
        case _:
            raise ValueError

    async with get_async_session() as session:
        res = await session.execute(
            text(
                f'''
                select
                {extract_clause},
                lineage_name,
                lineage_system_name,
                count(*)
                from (
                        select
                        {group_by},
                        lineage_name,
                        lineage_system_name
                        from samples_lineages sl
                        inner join lineages l on l.id = sl.lineage_id
                        inner join lineage_systems ls on ls.id = l.lineage_system_id
                        inner join samples s on s.id = sl.sample_id
                        {where_clause}
                )
                {group_and_order_clause}
                '''
            )
        )

    out_data = dict()
    for r in res:
        date = date_bin.format_iso_chunk(r[0], r[1])
        count = r[4]
        lineage = r[2]
        system = r[3]

        try:
            out_data[date][system][lineage] = count
        except KeyError:
            if date not in out_data.keys():
                out_data[date] = {system: {lineage: count}}
            elif system not in out_data[date].keys():
                out_data[date][system] = {lineage: count}
    return out_data


async def count_lineages_by_collection_date(
    date_bin: DateBinOpt,
    raw_query: str,
    days: int,
    max_span_days: int
) -> Dict[str, Dict[str, Dict[str, int]]]:
    user_where_clause = ''
    if raw_query is not None:
        user_where_clause = f'where {parser.parse(raw_query)}'

    match date_bin:
        case DateBinOpt.week | DateBinOpt.month:
            extract_clause = f'''
            extract(year from mid_collection_date) as year,
            extract({date_bin} from mid_collection_date) as chunk
            '''

            date_cols_to_group_by = f'year, chunk'

        case DateBinOpt.day:
            origin = datetime.date.today()
            extract_clause = f'''
            date_bin('{days} days', mid_collection_date, '{origin}') + interval '{days} days' as bin_end,
            date_bin('{days} days', mid_collection_date, '{origin}') as bin_start
            '''

            date_cols_to_group_by = 'bin_start'

        case _:
            raise ValueError

    async with get_async_session() as session:
        res = await session.execute(
            text(
                f'''
                select
                {extract_clause},
                lineage_name,
                lineage_system_name,
                count(*)
                from (
                    select
                    *,
                    (collection_start_date + ((collection_end_date - collection_start_date) / 2))::date AS mid_collection_date
                    from (
                        select
                        lineage_name,
                        lineage_system_name,
                        collection_start_date,
                        collection_end_date,
                        collection_end_date - collection_start_date as collection_span
                        from samples_lineages sl
                        inner join lineages l on l.id = sl.lineage_id
                        inner join lineage_systems ls on ls.id = l.lineage_system_id
                        inner join samples s on s.id = sl.sample_id
                        {user_where_clause}
                    )
                    where collection_span <= {max_span_days}
                )
                group by {date_cols_to_group_by}, lineage_name, lineage_system_name
                order by {date_cols_to_group_by}
                '''
            )
        )

    out_data = dict()
    for r in res:
        date = date_bin.format_iso_chunk(r[0], r[1])
        count = r[4]
        lineage = r[2]
        system = r[3]

        try:
            out_data[date][system][lineage] = count
        except KeyError:
            if date not in out_data.keys():
                out_data[date] = {system: {lineage: count}}
            elif system not in out_data[date].keys():
                out_data[date][system] = {lineage: count}
    return out_data
