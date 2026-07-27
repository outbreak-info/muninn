from typing import Type, List, Any, Dict

from sqlalchemy import text, select, Result
from sqlalchemy.sql.functions import func

from DB.engine import get_async_session
from DB.models import Sample, GeoLocation, IntraHostVariant, AminoAcid, Allele, Mutation, IntraHostTranslation, \
    MutationTranslation
from DB.queries.date_count_helpers import get_extract_clause, get_group_by_clause, get_order_by_cause, \
    MID_COLLECTION_DATE_CALCULATION
from DB.queries.helpers import get_appropriate_translations_table_and_id
from parser.parser import parser
from utils.constants import DateBinOpt, NtOrAa, ColumnNames, COLLECTION_DATE


async def count_samples_by_column(by_col: str, filter: str | None = None):
    where_clause = ''
    if filter is not None:
        where_clause = f'where {parser.parse(filter)}'

    query = f'''
        select {by_col}, count(*) as count1
        from samples s
        left join geo_locations gl on gl.id = s.geo_location_id
        {where_clause}
        group by {by_col}
        order by count1 desc
    '''
    async with get_async_session() as session:
        res = await session.execute(text(query))
        return await _package_count_by_column(res)


async def count_variants_by_column(by_col: str):
    async with get_async_session() as session:
        res = await session.execute(
            select(IntraHostVariant, Allele, IntraHostTranslation, AminoAcid)
            .join(Allele, Allele.id == IntraHostVariant.allele_id, isouter=True)
            .join(IntraHostTranslation, IntraHostTranslation.intra_host_variant_id == IntraHostVariant.id, isouter=True)
            .join(AminoAcid, AminoAcid.id == IntraHostTranslation.amino_acid_id, isouter=True)
            .with_only_columns(text(by_col), func.count().label('count1'))
            .group_by(text(by_col))
            .order_by(text('count1 desc'))
        )
        return await _package_count_by_column(res)


async def count_mutations_by_column(by_col: str, change_bin: NtOrAa = NtOrAa.aa, filter: str | None = None):
    if change_bin == NtOrAa.nt:
        cns_table, join_table, join_key = 'cns_samples_by_allele', 'alleles', 'allele_id'
        transposed_table, present_col = 'cns_alleles_by_sample', 'alleles_present'
    else:
        cns_table, join_table, join_key = 'cns_samples_by_amino_acid', 'amino_acids', 'amino_acid_id'
        transposed_table, present_col = 'cns_amino_acids_by_sample', 'amino_acids_present'

    if filter is None:
        query = f'''
            select {by_col}, sum(rb_cardinality(m.samples_present))::bigint as count1
            from {cns_table} m
            inner join {join_table} t on t.id = m.{join_key}
            group by {by_col}
            order by count1 desc
            '''
    else:
        query = f'''
            select {by_col}, count(*)::bigint as count1
            from {transposed_table} cs
            cross join lateral unnest(rb_to_array(cs.{present_col})) as u({join_key})
            inner join {join_table} t on t.id = u.{join_key}
            where cs.sample_id in (
                select s.id
                from samples s
                left join geo_locations gl on gl.id = s.geo_location_id
                where {parser.parse(filter)}
            )
            group by {by_col}
            order by count1 desc
            '''

    async with get_async_session() as session:
        res = await session.execute(text(query))
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

    extract_clause = get_extract_clause(group_by, date_bin, days)
    group_by_clause = get_group_by_clause(date_bin)
    order_by_clause = get_order_by_cause(date_bin)

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
                {group_by_clause}
                {order_by_clause}
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

    extract_clause = get_extract_clause(COLLECTION_DATE, date_bin, days)
    group_by_clause = get_group_by_clause(date_bin)
    order_by_clause = get_order_by_cause(date_bin)

    async with get_async_session() as session:
        res = await session.execute(
            text(
                f'''
                select
                    {extract_clause},
                    count(*)
                from (
                    select
                    {MID_COLLECTION_DATE_CALCULATION}
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
               {group_by_clause}
               {order_by_clause}
                '''
            )
        )
    out_data = dict()
    for r in res:
        date = date_bin.format_iso_chunk(r[0], r[1])
        out_data[date] = r[2]
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
    filter: str | None = None
):
    if change_bin == NtOrAa.nt:
        transposed_table, present_col, join_table, join_key = \
            'cns_alleles_by_sample', 'alleles_present', 'alleles', 'allele_id'
        feature_col, ref_col, pos_col, alt_col = 'region', 'ref_nt', 'position_nt', 'alt_nt'
    else:
        transposed_table, present_col, join_table, join_key = \
            'cns_amino_acids_by_sample', 'amino_acids_present', 'amino_acids', 'amino_acid_id'
        feature_col, ref_col, pos_col, alt_col = 'gff_feature', 'ref_aa', 'position_aa', 'alt_aa'

    user_defined_filter = ''
    if filter is not None:
        user_defined_filter = f'and ({parser.parse(filter)})'

    extract_clause = get_extract_clause(COLLECTION_DATE, date_bin, days)
    group_by_clause = get_group_by_clause(date_bin, [feature_col, ref_col, pos_col, alt_col])
    order_by_clause = get_order_by_cause(date_bin)

    async with get_async_session() as session:
        res = await session.execute(
            text(
                f'''
                select
                {extract_clause},
                count(distinct sample_id),
                {feature_col}, {ref_col}, {pos_col}, {alt_col}
                from (
                    select
                    *,
                    {MID_COLLECTION_DATE_CALCULATION}
                    from (
                        select
                            s.id as sample_id,
                            c.{feature_col}, c.{ref_col}, c.{pos_col}, c.{alt_col},
                            s.collection_start_date, s.collection_end_date,
                            s.collection_end_date - s.collection_start_date as collection_span
                        from samples s
                        left join geo_locations gl on gl.id = s.geo_location_id
                        left join samples_lineages sl on sl.sample_id = s.id
                        left join lineages l on l.id = sl.lineage_id
                        left join lineage_systems ls on ls.id = l.lineage_system_id
                        inner join {transposed_table} t on t.sample_id = s.id
                        cross join lateral unnest(rb_to_array(t.{present_col})) as u({join_key})
                        inner join {join_table} c on c.id = u.{join_key}
                        where num_nulls(s.collection_end_date, s.collection_start_date) = 0 {user_defined_filter}
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
        count = r[2]
        feature = r[3]
        ref = r[4]
        pos = r[5]
        alt = r[6]
        change_name = f'{feature}:{ref}{pos}{alt}'
        try:
            out_data[date][change_name] = count
        except KeyError:
            out_data[date] = {change_name: count}
    return out_data


# TODO: Generalize for nucleotide mutations
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

    extract_clause = get_extract_clause(COLLECTION_DATE, date_bin, days)
    group_by_clause = get_group_by_clause(
        date_bin,
        [ColumnNames.gff_feature, f'ref_{change_bin}', f'position_{change_bin}', f'alt_{change_bin}']
    )
    order_by_clause = get_order_by_cause(date_bin)

    translations_table, translations_join_col = get_appropriate_translations_table_and_id(table)

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
                    {MID_COLLECTION_DATE_CALCULATION}
                    from (
                        select
                        gff_feature, {change_fields},
                        collection_start_date, collection_end_date,
                        collection_end_date - collection_start_date as collection_span
                        from samples s
                        left join geo_locations gl on gl.id = s.geo_location_id
                        inner join {table.__tablename__} VM on VM.sample_id = s.id
                        left join {translations_table} t on t.{translations_join_col} = VM.id
                        left join amino_acids aas on aas.id = t.amino_acid_id
                        left join samples_lineages sl on sl.sample_id = s.id
                        left join lineages l on l.id = sl.lineage_id
                        left join lineage_systems ls on ls.id = l.lineage_system_id
                        where num_nulls(collection_end_date, collection_start_date) = 0 {user_where_clause}
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
    raw_query: str | None,
    days: int
) -> Dict[str, Dict[str, Dict[str, int]]]:
    where_clause = ''
    if raw_query is not None:
        where_clause = f'where {parser.parse(raw_query)}'

    extract_clause = get_extract_clause(group_by, date_bin, days)
    group_by_clause = get_group_by_clause(
        date_bin,
        [ColumnNames.lineage_name, ColumnNames.lineage_system_name]
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
                        left join geo_locations gl on gl.id = s.geo_location_id
                        {where_clause}
                )
                {group_by_clause}
                {order_by_clause}
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
    raw_query: str | None,
    days: int,
    max_span_days: int
) -> Dict[str, Dict[str, Dict[str, int]]]:
    user_where_clause = ''
    if raw_query is not None:
        user_where_clause = f'and {parser.parse(raw_query)}'

    extract_clause = get_extract_clause(COLLECTION_DATE, date_bin, days)
    group_by_clause = get_group_by_clause(
        date_bin,
        [ColumnNames.lineage_name, ColumnNames.lineage_system_name]
    )
    order_by_clause = get_order_by_cause(date_bin)

    async with get_async_session() as session:
        res = await session.execute(
            text(
               f'''
               with lin_samp_date as (
                    select lineage_name,
                           lineage_system_name,
                           collection_start_date,
                           collection_end_date,
                           collection_end_date - collection_start_date as collection_span,
                           {MID_COLLECTION_DATE_CALCULATION}
                    from samples_lineages sl
                    inner join lineages l on l.id = sl.lineage_id
                    inner join lineage_systems ls on ls.id = l.lineage_system_id
                    inner join samples s on s.id = sl.sample_id
                    left join geo_locations gl on gl.id = s.geo_location_id
                    where collection_end_date - collection_start_date <= {max_span_days} {user_where_clause}
                )
                select {extract_clause},
                       lineage_name,
                       lineage_system_name,
                       count(*)
                from lin_samp_date
                {group_by_clause}
                {order_by_clause};
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
