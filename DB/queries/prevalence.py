from typing import List, Type

from sqlalchemy import select, and_, ColumnElement, text, func

from DB.engine import get_async_session
from DB.models import IntraHostVariant, Sample, Allele, AminoAcid, Mutation, IntraHostTranslation, MutationTranslation
from DB.queries.helpers import get_appropriate_translations_table_and_id
from api.models import VariantFreqInfo, VariantCountPhenoScoreInfo, MutationCountInfo
from parser.parser import parser
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


# todo: I think the queries here need to be double-checked
async def get_mutation_sample_count_by_nt(change: str) -> List[MutationCountInfo]:
    region, ref_nt, position_nt, alt_nt = parse_change_string(change)

    where_clause = and_(
        Allele.region == region,
        Allele.ref_nt == ref_nt,
        Allele.position_nt == position_nt,
        Allele.alt_nt == alt_nt
    )

    return await _get_mutation_sample_count(where_clause)


async def get_mutation_sample_count_by_aa(change: str) -> List[MutationCountInfo]:
    region, ref_aa, position_aa, alt_aa = parse_change_string(change)

    where_clause = and_(
        Allele.region == region,
        AminoAcid.ref_aa == ref_aa,
        AminoAcid.position_aa == position_aa,
        AminoAcid.alt_aa == alt_aa
    )

    return await _get_mutation_sample_count(where_clause)


async def _get_mutation_sample_count(where_clause: ColumnElement[bool]) -> List[MutationCountInfo]:
    query = (
        select(Sample, Mutation, Allele.id, MutationTranslation.id, AminoAcid.id)
        .join(Mutation, Sample.id == Mutation.sample_id, isouter=True)
        .join(Allele, Allele.id == Mutation.allele_id, isouter=True)
        .join(MutationTranslation, MutationTranslation.mutation_id == Mutation.id, isouter=True)
        .join(AminoAcid, AminoAcid.id == MutationTranslation.amino_acid_id, isouter=True)
        .with_only_columns(Allele.id, MutationTranslation.id, AminoAcid.id, func.count())
        .group_by(Allele.id, MutationTranslation.id, AminoAcid.id)
        .where(where_clause)
    )

    async with get_async_session() as session:
        res = await session.execute(query)
    out_data = []
    for r in res:
        out_data.append(
            MutationCountInfo(
                allele_id=r[0],
                translation_id=r[1],
                amino_sub_id=r[2],
                sample_count=r[3]
            )
        )
    return out_data


async def get_pheno_values_and_mutation_counts(
    pheno_metric_name: str,
    region: str,
    include_refs: bool,
    samples_query: str | None
) -> List['VariantCountPhenoScoreInfo']:
    return await _get_pheno_values_and_counts(pheno_metric_name, region, Mutation, include_refs, samples_query)


async def get_pheno_values_and_variant_counts(
    pheno_metric_name: str,
    region: str,
    include_refs: bool,
    samples_query: str | None
) -> List['VariantCountPhenoScoreInfo']:
    return await _get_pheno_values_and_counts(pheno_metric_name, region, IntraHostVariant, include_refs, samples_query)


# TODO: Using "region" as the parameter for "gff_feature" for now.
async def _get_pheno_values_and_counts(
    pheno_metric_name: str,
    region: str,
    intermediate: Type[Mutation] | Type[IntraHostVariant],
    include_refs: bool,
    samples_query: str | None = None
) -> List['VariantCountPhenoScoreInfo']:
    tablename = intermediate.__tablename__

    no_refs_filter = f'and aas.ref_aa <> aas.alt_aa'
    if include_refs:
        no_refs_filter = ''

    samples_query_addin = '' if samples_query is None else f'and {parser.parse(samples_query)}'

    translations_table, translations_join_id = get_appropriate_translations_table_and_id(intermediate)

    async with get_async_session() as session:
        res = await session.execute(
            text(
                f'''
                select aas.ref_aa, aas.position_aa, aas.alt_aa, pmv.value, count(distinct s.id) as count
                from {tablename} TAB
                left join {translations_table} t on t.{translations_join_id} = TAB.id
                left join samples s on s.id = TAB.sample_id
                left join amino_acids aas on aas.id = t.amino_acid_id
                left join phenotype_metric_values pmv on pmv.amino_acid_id = aas.id
                left join phenotype_metrics pm on pm.id = pmv.phenotype_metric_id
                left join geo_locations gl on gl.id = s.geo_location_id
                where aas.gff_feature = :region and pm.name = :pm_name {no_refs_filter}
                {samples_query_addin}
                group by aas.ref_aa, aas.position_aa, aas.alt_aa, pmv.value
                order by count desc;
                '''
            ),
            {
                'region': region,
                'pm_name': pheno_metric_name
            }
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
                    count=count
                )
            )
    return out_data
