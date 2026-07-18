from typing import List

from sqlalchemy import select, text
from sqlalchemy.orm import contains_eager

from DB.engine import get_async_session
from DB.models import Sample, IntraHostVariant, Allele, AminoAcid, GeoLocation, IntraHostTranslation
from DB.queries.date_count_helpers import get_extract_clause, get_group_by_clause, get_order_by_cause, \
    MID_COLLECTION_DATE_CALCULATION
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


# TODO: Generalize this for nucleotide mutations
async def get_aa_variant_frequency_by_collection_date(
    date_bin: DateBinOpt,
    days: int,
    max_span_days: int,
    raw_query: str
):
    user_where_clause = ''
    if raw_query is not None:
        user_where_clause = f'and ({parser.parse(raw_query)})'

    extract_clause = get_extract_clause(COLLECTION_DATE, date_bin, days)
    group_by_clause = get_group_by_clause(
        date_bin,
        prefix_cols=[
            ColumnNames.gff_feature,
            ColumnNames.ref_aa,
            ColumnNames.position_aa,
            ColumnNames.alt_aa
        ]
    )
    order_by_clause = get_order_by_cause(date_bin)

    async with get_async_session() as session:
        res = await session.execute(
            text(
                f'''
                select
                {extract_clause},
                count(distinct sample_id) as n,
                percentile_cont(0.25) within group (order by alt_freq) as q1,
                percentile_cont(0.5) within group (order by alt_freq) as median,
                percentile_cont(0.75) within group (order by alt_freq) as q3,
                gff_feature,
                ref_aa,
                position_aa,
                alt_aa
                from(
                    select
                    gff_feature,
                    ref_aa,
                    position_aa,
                    alt_aa,
                    alt_freq,
                    sample_id,
                    {MID_COLLECTION_DATE_CALCULATION}
                    from (
                        select
                            aa.gff_feature,
                            aa.ref_aa,
                            aa.position_aa,
                            aa.alt_aa,
                            ihv.alt_freq,
                            s.id as sample_id,
                            s.collection_start_date,
                            s.collection_end_date,
                            collection_end_date - collection_start_date as collection_span
                        from samples s
                        inner join intra_host_variants ihv on ihv.sample_id = s.id
                        inner join {TableNames.intra_host_translations} t on t.{ColumnNames.intra_host_variant_id} = ihv.id
                        inner join amino_acids aa on aa.id = t.amino_acid_id
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
                "gff_feature": r[6],
                "ref_aa": r[7],
                "position_aa": r[8],
                "alt_aa": r[9]
            }
        )
    return out_data
