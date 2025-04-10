import re
from typing import List, Type

from sqlalchemy import select, and_, ColumnElement, text, func
from sqlalchemy.orm import Session

from DB.engine import engine
from DB.models import IntraHostVariant, Sample, Allele, AminoAcidSubstitution, Translation, Mutation
from api.models import VariantFreqInfo, VariantCountPhenoScoreInfo, MutationCountInfo
from parser.parser import parser
from utils.constants import CHANGE_PATTERN


def get_samples_variant_freq_by_aa_change(change: str) -> List[VariantFreqInfo]:
    region, ref_aa, position_aa, alt_aa = _parse_change_string(change)

    where_clause = and_(
        Allele.region == region,
        AminoAcidSubstitution.ref_aa == ref_aa,
        AminoAcidSubstitution.position_aa == position_aa,
        AminoAcidSubstitution.alt_aa == alt_aa
    )

    return _get_samples_variant_freq(where_clause)


def get_samples_variant_freq_by_nt_change(change: str) -> List[VariantFreqInfo]:
    region, ref_nt, position_nt, alt_nt = _parse_change_string(change)

    where_clause = and_(
        Allele.region == region,
        Allele.ref_nt == ref_nt,
        Allele.position_nt == position_nt,
        Allele.alt_nt == alt_nt
    )

    return _get_samples_variant_freq(where_clause)


def _get_samples_variant_freq(where_clause: ColumnElement[bool]) -> List[VariantFreqInfo]:
    query = (
        select(IntraHostVariant.alt_freq, Sample.accession, Allele.id, Translation.id, AminoAcidSubstitution.id)
        .join(Sample, Sample.id == IntraHostVariant.sample_id, isouter=True)
        .join(Allele, Allele.id == IntraHostVariant.allele_id, isouter=True)
        .join(Translation, Allele.id == Translation.allele_id, isouter=True)
        .join(AminoAcidSubstitution, Translation.amino_acid_substitution_id == AminoAcidSubstitution.id, isouter=True)
        .where(where_clause)
    )

    with Session(engine) as session:
        res = session.execute(query).all()
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


def get_mutation_sample_count_by_nt(change: str) -> List[MutationCountInfo]:
    region, ref_nt, position_nt, alt_nt = _parse_change_string(change)

    where_clause = and_(
        Allele.region == region,
        Allele.ref_nt == ref_nt,
        Allele.position_nt == position_nt,
        Allele.alt_nt == alt_nt
    )

    return _get_mutation_sample_count(where_clause)


def get_mutation_sample_count_by_aa(change: str) -> List[MutationCountInfo]:
    region, ref_aa, position_aa, alt_aa = _parse_change_string(change)

    where_clause = and_(
        Allele.region == region,
        AminoAcidSubstitution.ref_aa == ref_aa,
        AminoAcidSubstitution.position_aa == position_aa,
        AminoAcidSubstitution.alt_aa == alt_aa
    )

    return _get_mutation_sample_count(where_clause)


def _get_mutation_sample_count(where_clause: ColumnElement[bool]) -> List[MutationCountInfo]:
    query = (
        select(Sample, Mutation, Allele.id, Translation.id, AminoAcidSubstitution.id)
        .join(Mutation, Sample.id == Mutation.sample_id, isouter=True)
        .join(Allele, Allele.id == Mutation.allele_id, isouter=True)
        .join(Translation, Allele.id == Translation.allele_id, isouter=True)
        .join(AminoAcidSubstitution, Translation.amino_acid_substitution_id == AminoAcidSubstitution.id, isouter=True)
        .with_only_columns(Allele.id, Translation.id, AminoAcidSubstitution.id, func.count())
        .group_by(Allele.id, Translation.id, AminoAcidSubstitution.id)
        .where(where_clause)
    )

    with Session(engine) as session:
        res = session.execute(query).all()
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


def _parse_change_string(change: str) -> (str, str, int, str):
    pattern = re.compile(CHANGE_PATTERN)
    match = pattern.fullmatch(change)

    if match is None:
        raise ValueError(f'This change string fails validation: {change}')

    region = match[1]
    ref = match[2]
    position = int(match[3])
    alt = match[4]

    return region, ref, position, alt


def get_pheno_values_and_mutation_counts(
    pheno_metric_name: str,
    region: str,
    include_refs: bool,
    samples_query: str | None
) -> List['VariantCountPhenoScoreInfo']:
    return _get_pheno_values_and_counts(pheno_metric_name, region, Mutation, include_refs, samples_query)


def get_pheno_values_and_variant_counts(
    pheno_metric_name: str,
    region: str,
    include_refs: bool,
    samples_query: str | None
) -> List['VariantCountPhenoScoreInfo']:
    return _get_pheno_values_and_counts(pheno_metric_name, region, IntraHostVariant, include_refs, samples_query)


def _get_pheno_values_and_counts(
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

    samples_query_addin = ''
    if samples_query is not None:
        samples_query_addin = f'''and TAB_by_allele.sample_id in (
            select s.id from samples s 
            left join geo_locations gl on gl.id = s.geo_location_id
            where {parser.parse(samples_query)}
        )
        '''

    with Session(engine) as session:
        res = session.execute(
            text(
                f'''
                select aas.ref_aa, aas.position_aa, aas.alt_aa, pmr.value, (select 
                    count(1) from {tablename} TAB_by_allele 
                    where TAB_by_allele.allele_id = TAB.allele_id {samples_query_addin}
                ) as count
                from (select distinct allele_id from {tablename}) TAB
                left join alleles a on a.id = TAB.allele_id
                left join translations t on t.allele_id = a.id
                left join amino_acid_substitutions aas on aas.id = t.amino_acid_substitution_id
                left join phenotype_measurement_results pmr on pmr.amino_acid_substitution_id = aas.id
                left join phenotype_metrics pm on pm.id = pmr.phenotype_metric_id
                where  a.region = :region and pm.name = :pm_name {no_refs_filter}
                order by count desc;
                '''
            ),
            {
                'region': region,
                'pm_name': pheno_metric_name
            }
        ).all()

    out_data = []
    for r in res:
        out_data.append(
            VariantCountPhenoScoreInfo(
                ref_aa=r[0],
                position_aa=r[1],
                alt_aa=r[2],
                pheno_value=r[3],
                count=r[4]
            )
        )
    return out_data