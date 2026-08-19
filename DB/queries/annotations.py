from collections import defaultdict
from typing import List, Dict

from sqlalchemy import text

from DB.engine import get_async_session
from DB.queries.date_count_helpers import get_extract_clause, get_group_by_clause, get_order_by_cause, \
    MID_COLLECTION_DATE_CALCULATION, YEAR, CHUNK, BIN_START, BIN_END
from parser.parser import parser
from utils.constants import DateBinOpt, COLLECTION_DATE, TableNames, ColumnNames


async def get_all_annotation_effects() -> List[str]:
    query = f'select distinct {ColumnNames.detail} from {TableNames.effects}'
    async with get_async_session() as session:
        res = await session.execute(text(query))
        out_data = [r[0] for r in res]
    return out_data


async def _annotations_by_collection_date(
    effect_detail: str,
    date_bin: DateBinOpt,
    days: int,
    max_span_days: int,
    filter: str | None,
    intra_host: bool,
) -> List[Dict]:
    user_defined_filter = ''
    if filter is not None:
        user_defined_filter = f'and ({parser.parse(filter)})'

    # Which samples carry each annotated change, as (aa_id, bitmap). This is the only thing that
    # separates the consensus and intra-host halves of this endpoint; the rest of the query is shared.
    if intra_host:
        # This table is keyed (amino_acid_id, alt_freq_range) — one row per 0.05-wide frequency bin —
        # so the bins are OR-ed into one bitmap per change first. Summing per-bin cardinalities would
        # count a sample seen in two bins twice, and would inflate the denominator below by treating
        # each bin as its own change.
        carriers_cte = f'''carriers as (
            select v.{ColumnNames.amino_acid_id} as aa_id,
                   rb_or_agg(v.{ColumnNames.samples_present}) as bm
            from {TableNames.ih_samples_by_amino_acid} v
            inner join annotated ann on ann.aa_id = v.{ColumnNames.amino_acid_id}
            group by v.{ColumnNames.amino_acid_id}
        )'''
    else:
        # The cns table already has one row per change, so it needs no aggregation.
        carriers_cte = f'''carriers as (
            select m.{ColumnNames.amino_acid_id} as aa_id,
                   m.{ColumnNames.samples_present} as bm
            from {TableNames.cns_samples_by_amino_acid} m
            inner join annotated ann on ann.aa_id = m.{ColumnNames.amino_acid_id}
        )'''

    extract_clause = get_extract_clause(COLLECTION_DATE, date_bin, days)
    group_by_clause = get_group_by_clause(date_bin)
    order_by_clause = get_order_by_cause(date_bin)

    match date_bin:
        case DateBinOpt.week | DateBinOpt.month:
            bin_select_cols = f'{YEAR}, {CHUNK}'
        case DateBinOpt.day:
            bin_select_cols = f'{BIN_END}, {BIN_START}'
        case _:
            raise NotImplementedError

    query = f'''
        with binned_samples as (
            select s.id as sample_id,
                {MID_COLLECTION_DATE_CALCULATION}
            from {TableNames.samples} s
            left join {TableNames.geo_locations} gl on gl.id = s.{ColumnNames.geo_location_id}
            inner join {TableNames.samples_lineages} sl on sl.{ColumnNames.sample_id} = s.id
            inner join {TableNames.lineages} l on l.id = sl.{ColumnNames.lineage_id}
            inner join {TableNames.lineage_systems} ls on ls.id = l.{ColumnNames.lineage_system_id}
            where num_nulls({ColumnNames.collection_end_date}, {ColumnNames.collection_start_date}) = 0
              and ({ColumnNames.collection_end_date} - {ColumnNames.collection_start_date}) <= :max_span_days
              {user_defined_filter}
        ),
        bins as (
            select {extract_clause},
                   rb_build_agg(sample_id) as bm
            from binned_samples
            {group_by_clause}
        ),
        annotated as (
            select aaa.{ColumnNames.amino_acid_id} as aa_id,
                   bool_or(e.{ColumnNames.detail} = :effect_detail) as has_effect
            from {TableNames.annotations_amino_acids} aaa
            inner join {TableNames.annotations} a on a.id = aaa.{ColumnNames.annotation_id}
            inner join {TableNames.effects} e on e.id = a.{ColumnNames.effect_id}
            inner join {TableNames.annotations_papers} ap on ap.{ColumnNames.annotation_id} = a.id
            group by aaa.{ColumnNames.amino_acid_id}
        ),
        {carriers_cte},
        per as (
            select {bin_select_cols},
                   ann.has_effect as has_effect,
                   rb_and_cardinality(c.bm, b.bm) as card
            from bins b
            cross join annotated ann
            inner join carriers c on c.aa_id = ann.aa_id
        )
        select {bin_select_cols},
               count(*) filter (where card > 0 and has_effect) as n,
               count(*) filter (where card > 0) as n_total,
               (count(*) filter (where card > 0 and has_effect))::numeric
                   / nullif(count(*) filter (where card > 0), 0) as proportion
        from per
        {group_by_clause}
        having count(*) filter (where card > 0) > 0
        {order_by_clause}
    '''
    async with get_async_session() as session:
        res = await session.execute(
            text(query),
            {
                'effect_detail': effect_detail,
                'max_span_days': max_span_days,
            }
        )
        rows = res.all()
    out_data = []
    for r in rows:
        date = date_bin.format_iso_chunk(r[0], r[1])
        out_data.append(
            {
                "date": date,
                "n": r[2],
                "n_total": r[3],
                "proportion": r[4]
            }
        )
    return out_data


async def get_annotations_by_mutations_and_collection_date(
    effect_detail: str,
    date_bin: DateBinOpt,
    days: int,
    max_span_days: int,
    filter: str | None,
) -> List[Dict]:
    return await _annotations_by_collection_date(
        effect_detail, date_bin, days, max_span_days, filter, intra_host=False
    )


async def get_annotations_by_variants_and_collection_date(
    effect_detail: str,
    date_bin: DateBinOpt,
    days: int,
    max_span_days: int,
    filter: str | None,
) -> List[Dict]:
    """
    The intra-host counterpart of :byMutationsAndCollectionDate. Identical shape and denominator —
    per date bin, how many of the annotated amino-acid changes present in that bin's samples carry
    the requested effect — but read off the intra-host table, so "present" means observed
    sub-consensus rather than fixed in the consensus sequence.
    """
    return await _annotations_by_collection_date(
        effect_detail, date_bin, days, max_span_days, filter, intra_host=True
    )


async def _annotations_by_amino_acid_position(
    effect_detail: str,
    filter: str | None,
    intra_host: bool,
) -> Dict:
    user_defined_filter = ''
    if filter is not None:
        user_defined_filter = f'and ({parser.parse(filter)})'

    # Which samples carry each annotated change, as (aa_id, bitmap). This is the only thing that
    # separates the consensus and intra-host halves of this endpoint; the rest of the query is shared.
    if intra_host:
        # This table is keyed (amino_acid_id, alt_freq_range) — one row per 0.05-wide frequency bin —
        # so the bins are OR-ed into one bitmap per change first. Summing per-bin cardinalities would
        # count a sample seen in two bins twice.
        carriers_cte = f'''carriers as (
            select v.{ColumnNames.amino_acid_id} as aa_id,
                   rb_or_agg(v.{ColumnNames.samples_present}) as bm
            from {TableNames.ih_samples_by_amino_acid} v
            inner join annotated ann on ann.aa_id = v.{ColumnNames.amino_acid_id}
            group by v.{ColumnNames.amino_acid_id}
        )'''
    else:
        # The cns table already has one row per change, so it needs no aggregation.
        carriers_cte = f'''carriers as (
            select m.{ColumnNames.amino_acid_id} as aa_id,
                   m.{ColumnNames.samples_present} as bm
            from {TableNames.cns_samples_by_amino_acid} m
            inner join annotated ann on ann.aa_id = m.{ColumnNames.amino_acid_id}
        )'''

    query = f'''
        with matching_bm as (
            select rb_build_agg(s.id) as bm
            from {TableNames.samples} s
            left join {TableNames.geo_locations} gl on gl.id = s.{ColumnNames.geo_location_id}
            inner join {TableNames.samples_lineages} sl on sl.{ColumnNames.sample_id} = s.id
            inner join {TableNames.lineages} l on l.id = sl.{ColumnNames.lineage_id}
            inner join {TableNames.lineage_systems} ls on ls.id = l.{ColumnNames.lineage_system_id}
            where true {user_defined_filter}
        ),
        annotated as (
            select aa.id as aa_id,
                   aa.{ColumnNames.gff_feature} as gff_feature,
                   aa.{ColumnNames.position_aa} as position_aa,
                   aa.{ColumnNames.alt_aa} as alt_aa,
                   aa.{ColumnNames.ref_aa} as ref_aa
            from {TableNames.amino_acids} aa
            inner join {TableNames.annotations_amino_acids} aaa on aaa.{ColumnNames.amino_acid_id} = aa.id
            inner join {TableNames.annotations} a on a.id = aaa.{ColumnNames.annotation_id}
            inner join {TableNames.effects} e on e.id = a.{ColumnNames.effect_id}
            inner join {TableNames.annotations_papers} ap on ap.{ColumnNames.annotation_id} = a.id
            where e.{ColumnNames.detail} = :effect_detail
            group by aa.id, aa.{ColumnNames.gff_feature}, aa.{ColumnNames.position_aa}, aa.{ColumnNames.alt_aa}, aa.{ColumnNames.ref_aa}
        ),
        {carriers_cte},
        per as (
            select ann.gff_feature, ann.position_aa, ann.alt_aa, ann.ref_aa,
                   rb_and_cardinality(c.bm, (select bm from matching_bm)) as card
            from annotated ann
            inner join carriers c on c.aa_id = ann.aa_id
        )
        select gff_feature, position_aa, alt_aa, ref_aa, sum(card) as count
        from per
        group by gff_feature, position_aa, alt_aa, ref_aa
        having sum(card) > 0
    '''
    async with get_async_session() as session:
        res = await session.execute(text(query), {'effect_detail': effect_detail})
        rows = res.all()
    out = defaultdict(list)
    for r in rows:
        gff_feature, position_aa, alt_aa, ref_aa, count = r
        out[gff_feature].append(
            {
                "position_aa": position_aa,
                "alt_aa": alt_aa,
                "ref_aa": ref_aa,
                "count": int(count)
            }
        )
    return out


async def get_annotations_by_variants_and_amino_acid_position(
    effect_detail: str,
    filter: str | None
) -> Dict:
    """
    The intra-host counterpart of :byMutationsAndAminoAcidPosition. Same shape, same counting rule —
    a sample is counted once per annotated change it carries — but read off the intra-host table
    instead of the consensus one, so it counts samples carrying the change sub-consensus.
    """
    return await _annotations_by_amino_acid_position(effect_detail, filter, intra_host=True)


async def get_annotations_by_mutations_and_amino_acid_position(
    effect_detail: str,
    filter: str | None
) -> Dict:
    return await _annotations_by_amino_acid_position(effect_detail, filter, intra_host=False)
