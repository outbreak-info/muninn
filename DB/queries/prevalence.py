from typing import List, Type

from sqlalchemy import select, and_, ColumnElement, text, func

from DB.engine import get_async_session
from DB.models import IntraHostVariant, Sample, Allele, AminoAcid, Mutation, IntraHostTranslation, MutationTranslation
from DB.queries.helpers import get_appropriate_translations_table_and_id
from api.models import VariantFreqInfo, VariantCountPhenoScoreInfo, MutationCountInfo
from parser.parser import parser
from utils.constants import StandardColumnNames, TableNames
from utils.csv_helpers import parse_change_string


async def get_samples_variant_freq_by_aa_change(change: str) -> List[VariantFreqInfo]:
    region, ref_aa, position_aa, alt_aa = parse_change_string(change)

    where_clause = and_(
        Allele.region == region,
        AminoAcid.ref_aa == ref_aa,
        AminoAcid.position_aa == position_aa,
        AminoAcid.alt_aa == alt_aa
    )

    return await _get_samples_variant_freq(where_clause)


async def get_samples_variant_freq_by_nt_change(change: str) -> List[VariantFreqInfo]:
    region, ref_nt, position_nt, alt_nt = parse_change_string(change)

    where_clause = and_(
        Allele.region == region,
        Allele.ref_nt == ref_nt,
        Allele.position_nt == position_nt,
        Allele.alt_nt == alt_nt
    )

    return await _get_samples_variant_freq(where_clause)


async def _get_samples_variant_freq(where_clause: ColumnElement[bool]) -> List[VariantFreqInfo]:
    query = (
        select(IntraHostVariant.alt_freq, Sample.accession, Allele.id, IntraHostTranslation.id, AminoAcid.id)
        .join(Sample, Sample.id == IntraHostVariant.sample_id, isouter=True)
        .join(Allele, Allele.id == IntraHostVariant.allele_id, isouter=True)
        .join(IntraHostTranslation, IntraHostTranslation.intra_host_variant_id == IntraHostVariant.id, isouter=True)
        .join(AminoAcid, AminoAcid.id == IntraHostTranslation.amino_acid_id, isouter=True)
        .where(where_clause)
    )

    async with get_async_session() as session:
        res = await session.execute(query)
    out_data = []
    for r in res:
        out_data.append(
            VariantFreqInfo(
                alt_freq=r[0],
                accession=r[1],
                allele_id=r[2],
                translation_id=r[3],
                amino_sub_id=r[4]
            )
        )
    return out_data


async def get_mutation_sample_count_by_nt(change: str) -> List[MutationCountInfo]:
    region, ref_nt, position_nt, alt_nt = parse_change_string(change)

    async with get_async_session() as session:
        res = await session.execute(
            text(
                f"""
                  select
                      count(distinct s.id) as sample_count
                  from {TableNames.mutations} m
                  inner join {TableNames.alleles} a on a.id = m.{StandardColumnNames.allele_id}
                  cross join lateral unnest(rb_to_array(m.sequences_present)) as seqs({StandardColumnNames.sequence_id})
                  inner join {TableNames.samples} s on s.{StandardColumnNames.sequence_id} = seqs.{StandardColumnNames.sequence_id}
                  where a.{StandardColumnNames.region} = '{region}'
                    and a.{StandardColumnNames.ref_nt} = '{ref_nt}'
                    and a.{StandardColumnNames.position_nt} = {position_nt}
                    and a.alt_nt = '{alt_nt}'
                  group by m.allele_id
                  """
            )
        )
    return [
        MutationCountInfo(amino_sub_id=None, sample_count=r.sample_count) for r in res
    ]


async def get_mutation_sample_count_by_aa(change: str) -> List[MutationCountInfo]:
    gff_feature, ref_aa, position_aa, alt_aa = parse_change_string(change)

    async with get_async_session() as session:
        res = await session.execute(
            text(
                f"""
                  select
                      mt.{StandardColumnNames.amino_acid_id} as amino_sub_id,
                      count(distinct s.id) as sample_count
                  from {TableNames.mutation_translations} mt
                  inner join {TableNames.amino_acids} aa on aa.id = mt.{StandardColumnNames.amino_acid_id}
                  cross join lateral unnest(rb_to_array(mt.sequences_present)) as seqs({StandardColumnNames.sequence_id})
                  inner join {TableNames.samples} s on s.{StandardColumnNames.sequence_id} = seqs.{StandardColumnNames.sequence_id}
                  where aa.{StandardColumnNames.gff_feature} = '{gff_feature}'
                    and aa.{StandardColumnNames.ref_aa} = '{ref_aa}'
                    and aa.{StandardColumnNames.position_aa} = {position_aa}
                    and aa.{StandardColumnNames.alt_aa} = '{alt_aa}'
                  group by mt.{StandardColumnNames.amino_acid_id}
                  """
            )
        )
    return [
        MutationCountInfo(amino_sub_id=r.amino_sub_id, sample_count=r.sample_count)
        for r in res
    ]

async def get_pheno_values_and_mutation_counts(
    pheno_metric_name: str, region: str, include_refs: bool, samples_query: str | None
) -> List["VariantCountPhenoScoreInfo"]:
    no_refs_filter = "and aas.ref_aa <> aas.alt_aa"
    if include_refs:
        no_refs_filter = ""

    samples_query_addin = (
        "" if samples_query is None else f"and {parser.parse(samples_query)}"
    )

    # Consensus AA changes live in mutation_translations (one row per amino_acid, with a
    # `sequences_present` bitmap). Filter amino_acids/phenotype values first, then expand
    # only the matching bitmaps to sequences and count the distinct samples carrying each.
    async with get_async_session() as session:
        res = await session.execute(
            text(
                f"""
                select aas.ref_aa, aas.position_aa, aas.alt_aa, pmv.value, count(distinct s.id) as count
                from mutation_translations mt
                inner join amino_acids aas on aas.id = mt.amino_acid_id
                inner join phenotype_metric_values pmv on pmv.amino_acid_id = aas.id
                inner join phenotype_metrics pm on pm.id = pmv.phenotype_metric_id
                cross join lateral unnest(rb_to_array(mt.sequences_present)) as seqs(sequence_id)
                inner join samples s on s.sequence_id = seqs.sequence_id
                left join geo_locations gl on gl.id = s.geo_location_id
                where aas.gff_feature = :region
                and pm.{StandardColumnNames.phenotype_metric_name} = :pm_name
                {no_refs_filter}
                {samples_query_addin}
                group by aas.ref_aa, aas.position_aa, aas.alt_aa, pmv.value
                order by count desc;
                """
            ),
            {"region": region, "pm_name": pheno_metric_name},
        )

    out_data = []
    for r in res:
        count = r[4]
        if count > 0:
            out_data.append(
                VariantCountPhenoScoreInfo(
                    ref_aa=r[0],
                    position_aa=r[1],
                    alt_aa=r[2],
                    pheno_value=r[3],
                    count=count,
                )
            )
    return out_data


async def get_pheno_values_and_variant_counts(
    pheno_metric_name: str, region: str, include_refs: bool, samples_query: str | None
) -> List["VariantCountPhenoScoreInfo"]:
    return await _get_pheno_values_and_counts(
        pheno_metric_name, region, IntraHostVariant, include_refs, samples_query
    )


# TODO: Using "region" as the parameter for "gff_feature" for now.
async def _get_pheno_values_and_counts(
    pheno_metric_name: str,
    region: str,
    intermediate: Type[Mutation] | Type[IntraHostVariant],
    include_refs: bool,
    samples_query: str | None = None,
) -> List["VariantCountPhenoScoreInfo"]:
    tablename = intermediate.__tablename__

    no_refs_filter = f"and aas.ref_aa <> aas.alt_aa"
    if include_refs:
        no_refs_filter = ""

    samples_query_addin = (
        "" if samples_query is None else f"and {parser.parse(samples_query)}"
    )

    translations_table, translations_join_id = (
        get_appropriate_translations_table_and_id(intermediate)
    )

    async with get_async_session() as session:
        res = await session.execute(
            text(
                f"""
                select aas.ref_aa, aas.position_aa, aas.alt_aa, pmv.value, count(distinct s.id) as count
                from {tablename} TAB
                left join {translations_table} t on t.{translations_join_id} = TAB.id
                left join samples s on s.id = TAB.sample_id
                left join amino_acids aas on aas.id = t.amino_acid_id
                left join phenotype_metric_values pmv on pmv.amino_acid_id = aas.id
                left join phenotype_metrics pm on pm.id = pmv.phenotype_metric_id
                left join geo_locations gl on gl.id = s.geo_location_id
                where aas.gff_feature = :region
                and pm.{StandardColumnNames.phenotype_metric_name} = :pm_name
                {no_refs_filter}
                {samples_query_addin}
                group by aas.ref_aa, aas.position_aa, aas.alt_aa, pmv.value
                order by count desc;
                """
            ),
            {"region": region, "pm_name": pheno_metric_name},
        )

    out_data = []
    for r in res:
        count = r[4]
        if count > 0:
            out_data.append(
                VariantCountPhenoScoreInfo(
                    ref_aa=r[0],
                    position_aa=r[1],
                    alt_aa=r[2],
                    pheno_value=r[3],
                    count=count,
                )
            )
    return out_data
