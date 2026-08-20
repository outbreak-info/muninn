from typing import List, Annotated, Dict

from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy.exc import ProgrammingError

import DB.queries.annotations
import DB.queries.counts
import DB.queries.helpers
import DB.queries.lineages
import DB.queries.mutations
import DB.queries.phenotype_metrics
import DB.queries.prevalence
import DB.queries.samples
import DB.queries.variants
import DB.queries.variants_mutations
import DB.queries.wastewater
from DB.models import Mutation, IntraHostVariant
from api.models import LineageAbundanceWithSampleInfo, VariantNucleotideInfo, SampleInfo, MutationInfo, VariantFreqInfo, \
    VariantCountPhenoScoreInfo, \
    MutationCountInfo, PhenotypeMetricInfo, LineageCountInfo, LineageAbundanceInfo, LineageAbundanceSummaryInfo, \
    LineageInfo, VariantMutationLagInfo, RegionAndGffFeatureInfo, MutationProfileInfo, AverageLineageAbundanceInfo
from utils.constants import CHANGE_PATTERN, WORDLIKE_PATTERN, DateBinOpt, SIMPLE_DATE_FIELDS, NtOrAa, \
    DEFAULT_MAX_SPAN_DAYS, COLLECTION_DATE, DEFAULT_DAYS, COMMA_SEP_WORDLIKE_PATTERN, LINEAGE, \
    DEFAULT_PREVALENCE_THRESHOLD, MIN_PREVALENCE_THRESHOLD
from utils.errors import ParsingError

app = FastAPI(
    title='Muninn API',
    description='API for querying Muninn database',
    version='0.1.0'
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=['*'],
    allow_credentials=False,
    allow_methods=['*'],
    allow_headers=['*']
)

# Tag names used to group the endpoints in the auto-generated docs at /docs.
TAG_SAMPLES = 'Samples'
TAG_VARIANTS = 'Variants'
TAG_MUTATIONS = 'Mutations'
TAG_LINEAGES = 'Lineages'
TAG_WASTEWATER = 'Wastewater'
TAG_PHENOTYPE = 'Phenotype Metrics'
TAG_ANNOTATIONS = 'Annotations'

@app.get('/v0/sample/{sample_id}', response_model=SampleInfo, tags=[TAG_SAMPLES], summary='Get sample metadata by sample ID')
async def get_sample_by_id(sample_id: int):
    sample = await DB.queries.samples.get_sample_by_id(sample_id)
    if sample is None:
        raise HTTPException(status_code=404)
    return sample


@app.get('/v0/phenotype_metrics', response_model=List[PhenotypeMetricInfo], tags=[TAG_PHENOTYPE], summary='Get all available phenotype metrics')
async def get_all_phenotype_metrics():
    return await DB.queries.phenotype_metrics.get_all_pheno_metrics()


@app.get('/samples', response_model=List[SampleInfo], tags=[TAG_SAMPLES], summary='Get samples matching a query')
async def get_samples_query(q: str):
    try:
        return await DB.queries.samples.get_samples(q)
    except ParsingError as e:
        raise HTTPException(status_code=400, detail=e.message)


@app.get('/samples:collectionReleaseLag', response_model=List[Dict], tags=[TAG_SAMPLES], summary='Get sample collection to release lag statistics')
async def get_samples_query(max_span_days: int = DEFAULT_MAX_SPAN_DAYS):
    try:
        return await DB.queries.samples.get_sample_collection_release_lag(max_span_days)
    except ParsingError as e:
        raise HTTPException(status_code=400, detail=e.message)


@app.get('/variants', response_model=List[VariantNucleotideInfo], tags=[TAG_VARIANTS], summary='Get variants matching a query')
async def get_variants_query(q: str):
    try:
        return await DB.queries.variants.get_variants(q)
    except ParsingError as e:
        raise HTTPException(status_code=400, detail=e.message)


@app.get('/mutations', response_model=List[MutationInfo], tags=[TAG_MUTATIONS], summary='Get mutations matching a query')
async def get_mutations_query(q: str):
    try:
        return await DB.queries.mutations.get_mutations(q)
    except ParsingError as e:
        raise HTTPException(status_code=400, detail=e.message)


@app.get('/lineages', response_model=List[LineageInfo], tags=[TAG_LINEAGES], summary='Get lineages matching a query')
async def get_lineages_by_lineage_system(lineage_system_name: str):
    return await DB.queries.lineages.get_all_lineages_by_lineage_system(lineage_system_name)


@app.get('/variants/by/sample', response_model=List[VariantNucleotideInfo], tags=[TAG_VARIANTS], summary='Get variants for samples matching a query')
async def get_variants_by_sample(q: str):
    try:
        return await DB.queries.variants.get_variants_for_sample(q)
    except ParsingError as e:
        raise HTTPException(status_code=400, detail=e.message)


@app.get('/mutations/by/sample', response_model=List[MutationInfo], tags=[TAG_MUTATIONS], summary='Get mutations for samples matching a query')
async def get_mutations_by_sample(q: str):
    try:
        return await DB.queries.mutations.get_mutations_by_sample(q)
    except ParsingError as e:
        raise HTTPException(status_code=400, detail=e.message)


@app.get('/samples/by/mutation', response_model=List[SampleInfo], tags=[TAG_SAMPLES], summary='Get samples carrying a mutation matching a query')
async def get_samples_by_mutation(change_bin: NtOrAa = NtOrAa.aa, q: str = ""):
    try:
        return await DB.queries.samples.get_samples_by_mutation(change_bin, q)
    except ParsingError as e:
        raise HTTPException(status_code=400, detail=e.message)


@app.get('/samples/by/variant', response_model=List[SampleInfo], tags=[TAG_SAMPLES], summary='Get samples carrying a variant matching a query')
async def get_samples_by_variant(q: str):
    try:
        return await DB.queries.samples.get_samples_by_variant(q)
    except ParsingError as e:
        raise HTTPException(status_code=400, detail=e.message)


@app.get('/count/{x}/by/{y}', response_model=Dict[str, int], tags=[TAG_SAMPLES, TAG_VARIANTS, TAG_MUTATIONS], summary='Count rows of a table grouped by a column')
async def get_count_x_by_y(x: str, y: str):
    if x is None or y is None:
        raise HTTPException(status_code=400, detail='Provide target table and by-column')

    if not WORDLIKE_PATTERN.fullmatch(y):
        raise HTTPException(status_code=400, detail=f'This alleged column name fails validation: {y}')

    try:
        match x:
            case 'samples':
                return await DB.queries.counts.count_samples_by_column(y)
            case 'variants':
                return await DB.queries.counts.count_variants_by_column(y)
            case 'mutations':
                return await DB.queries.counts.count_mutations_by_column(y)
            case _:
                raise HTTPException(status_code=400, detail='counts are available for: samples, variants, mutations')
    except ProgrammingError as e:
        short_message = str(e).split('\n')[0]
        raise HTTPException(status_code=400, detail=short_message)


@app.get('/variants/frequency', response_model=List[VariantFreqInfo], tags=[TAG_VARIANTS], summary='Get per-sample variant frequency for a given mutation')
async def get_variant_frequency(
    aa: Annotated[
        str | None, Query(regex=CHANGE_PATTERN)
    ] = None,
    nt: Annotated[
        str | None, Query(regex=CHANGE_PATTERN)
    ] = None
):
    if aa is not None and nt is not None:
        raise HTTPException(status_code=400, detail='Provide either amino or nt change, not both')
    elif aa is not None:
        return await DB.queries.prevalence.get_samples_variant_freq_by_aa_change(aa)
    elif nt is not None:
        return await DB.queries.prevalence.get_samples_variant_freq_by_nt_change(nt)


# todo: actually a count
@app.get('/mutations/frequency', response_model=List[MutationCountInfo], tags=[TAG_MUTATIONS], summary='Count samples carrying a given mutation')
async def get_mutation_sample_count(
    aa: Annotated[
        str | None, Query(regex=CHANGE_PATTERN)
    ] = None,
    nt: Annotated[
        str | None, Query(regex=CHANGE_PATTERN)
    ] = None
):
    if aa is not None and nt is not None:
        raise HTTPException(status_code=400, detail='Provide either amino or nt change, not both')
    elif aa is not None:
        return await DB.queries.prevalence.get_mutation_sample_count_by_aa(aa)
    elif nt is not None:
        return await DB.queries.prevalence.get_mutation_sample_count_by_nt(nt)


# todo: actually a count
#  /count/samples/pheno_scores/variants
@app.get('/variants/frequency/score', response_model=List[VariantCountPhenoScoreInfo], tags=[TAG_VARIANTS], summary='Count samples per variant alongside a phenotype metric value')
async def get_variant_counts_by_phenotype_score(
    region: str,
    metric: str,
    include_refs: bool = False,
    q: str | None = None
):
    """
    :param region: (GFF feature) Results will include only variants in the given region
    :param metric: Phenotype metric whose values will be included in results
    :param include_refs: if true, include variants where ref aa = alt aa
    :param q: Query against samples. If provided, only samples matching this query will be included in the count
    """
    return await DB.queries.prevalence.get_pheno_values_and_variant_counts(metric, region, include_refs, q)


# todo: actually a count
#  /count/samples/pheno_scores/mutations
@app.get('/mutations/frequency/score', response_model=List[VariantCountPhenoScoreInfo], tags=[TAG_MUTATIONS], summary='Count samples per mutation alongside a phenotype metric value')
async def get_mutation_counts_by_phenotype_score(
    region: str,
    metric: str,
    include_refs: bool = False,
    q: str | None = None
):
    """
    :param region: (GFF feature) Results will include only mutations in the given region
    :param metric: Phenotype metric whose values will be included in results
    :param include_refs: if true, include mutations where ref aa = alt aa
    :param q: Query against samples. If provided, only samples matching this query will be included in the count
    """
    return await DB.queries.prevalence.get_pheno_values_and_mutation_counts(metric, region, include_refs, q)


# deprecated: use /v0/samples:count?group_by=lineage
@app.get('/count/samples/lineages', response_model=List[LineageCountInfo], tags=[TAG_LINEAGES], deprecated=True)
async def get_sample_counts_per_lineage(q: str | None = None):
    """
    :param q: A query to be run against samples. If provided, only samples matching the query will be counted.
    """
    try:
        return await DB.queries.lineages.get_sample_counts_by_lineage(q)
    except ParsingError as e:
        raise HTTPException(status_code=400, detail=e.message)


# deprecated: use /v0/lineages:abundance?summary=false
@app.get('/lineages/abundances', response_model=List[LineageAbundanceInfo], tags=[TAG_LINEAGES], deprecated=True)
async def get_lineage_abundance_info(q: str | None = None):
    """
    :param q: a query to be run against lineages and samples.
    Note that results without abundance numbers are always excluded.
    """
    try:
        return await DB.queries.lineages.get_abundances(raw_query=q)
    except ParsingError as e:
        raise HTTPException(status_code=400, detail=e.message)


@app.get('/v0/wastewater/lineages:averageAbundancesByLocation', response_model=List[AverageLineageAbundanceInfo], tags=[TAG_WASTEWATER], summary='Get population-weighted average lineage abundances by location and week')
async def get_average_lineage_abundances_by_location(
    q: str | None = None,
    geo_bin: str = "admin1_name",
    max_span_days: int = DEFAULT_MAX_SPAN_DAYS,
    lineage: str | None = None
):
    """
    Get average lineage abundances by location.

    :param q: A query to be run against lineages and samples.
    :param geo_bin: The geographic bin to group by.
    :param lineage: Optional lineage name. If it ends with '*', returns abundances for
                    the parent lineage and all its children aggregated together.
                    If not provided or doesn't end with '*', returns abundances for all lineages.
    :param max_span_days: The maximum span between collection start and end dates.
    """
    try:
        return await DB.queries.wastewater.get_averaged_lineage_abundances_by_location(
            q, geo_bin, max_span_days, lineage
        )
    except ParsingError as e:
        raise HTTPException(status_code=400, detail=e.message)


@app.get('/v0/wastewater/lineages:abundancesBySample', response_model=List[LineageAbundanceWithSampleInfo], tags=[TAG_WASTEWATER], summary='Get per-sample wastewater lineage abundances')
async def get_lineage_abundances_by_sample(
    q: str | None = None,
):
    """
    :param q: A query to be run against samples and lineages.
    """
    try:
        return await DB.queries.wastewater.get_lineage_abundances_by_sample(q)
    except ParsingError as e:
        raise HTTPException(status_code=400, detail=e.message)


@app.get('/v0/wastewater/latestSample', response_model=List[SampleInfo], tags=[TAG_WASTEWATER], summary='Get the most recently collected sample(s)')
async def get_latest_sample(q: str | None = None):
    """
    :param q: A query to be run against samples.
    """
    try:
        return await DB.queries.wastewater.get_latest_sample(q)
    except ParsingError as e:
        raise HTTPException(status_code=400, detail=e.message)

@app.get('/v0/wastwater/samples:countSamplesWithLineageData', response_model=Dict[str, int])
async def count_samples_with_lineages(
    group_by: Annotated[str, Query(regex=COMMA_SEP_WORDLIKE_PATTERN.pattern)],
    q: str | None = None,
):
    return await DB.queries.wastewater.count_samples_with_lineage_data(group_by, q)

@app.get('/v0/wastwater/lineages:countLineagesBySample', response_model=Dict[str, int])
async def count_samples_with_lineages(
    q: str | None = None,
):
    return await DB.queries.wastewater.count_lineages_by_sample_data(q)

# deprecated: use /v0/lineages:abundance (summary=true, the default)
@app.get('/lineages/abundances/summary_stats', response_model=List[LineageAbundanceSummaryInfo], tags=[TAG_LINEAGES], summary='Get lineage abundance summary statistics', deprecated=True)
async def get_lineage_abundance_summary_stats(q: str | None = None):
    try:
        return await DB.queries.lineages.get_abundance_summaries(q)
    except ParsingError as e:
        raise HTTPException(status_code=400, detail=e.message)


@app.get(
    '/v0/samples:count',
    response_model=Dict[str, int] | Dict[str, Dict[str, Dict[str, int]]] | List[LineageCountInfo],
    tags=[TAG_SAMPLES],
    summary='Count samples grouped by field, date, and/or lineage'
)
async def get_sample_counts(
    group_by: Annotated[str, Query(regex=COMMA_SEP_WORDLIKE_PATTERN.pattern)],
    date_bin: DateBinOpt = DateBinOpt.month,
    days: int = DEFAULT_DAYS,
    q: str | None = None,
    max_span_days: int = DEFAULT_MAX_SPAN_DAYS
):
    # allow grouping by lineage and date, this is an experiment
    group_by_set = set(group_by.split(','))
    if len(group_by_set) > 1:
        if len(group_by_set) > 2:
            raise HTTPException(status_code=400, detail='Max of 2 group_by values allowed')
        if LINEAGE in group_by_set:
            date_field = group_by_set.difference({LINEAGE}).pop()
            if date_field in SIMPLE_DATE_FIELDS:
                return await DB.queries.counts.count_lineages_by_simple_date(date_field, date_bin, q, days)
            elif date_field == COLLECTION_DATE:
                return await DB.queries.counts.count_lineages_by_collection_date(date_bin, q, days, max_span_days)

        raise HTTPException(
            status_code=501,
            detail='Grouping by multiple fields is currently only supported for lineage plus a date field'
        )
    else:
        if group_by in SIMPLE_DATE_FIELDS:
            return await DB.queries.counts.count_samples_by_simple_date(group_by, date_bin, days, q)
        elif group_by == COLLECTION_DATE:
            return await DB.queries.counts.count_samples_by_collection_date(date_bin, days, q, max_span_days)
        elif group_by == LINEAGE:
            return await DB.queries.lineages.get_sample_counts_by_lineage(q)
        else:
            return await DB.queries.counts.count_samples_by_column(group_by)


@app.get('/v0/variants:count', response_model=Dict[str, Dict[str, int]] | Dict[str, int], tags=[TAG_VARIANTS], summary='Count variants grouped by field or collection date')
async def get_variant_counts(
    group_by: Annotated[str, Query(regex=WORDLIKE_PATTERN.pattern)],
    date_bin: DateBinOpt = DateBinOpt.month,
    days: int = DEFAULT_DAYS,
    q: str | None = None,
    change_bin: NtOrAa = NtOrAa.aa,
    max_span_days: int = DEFAULT_MAX_SPAN_DAYS
):
    """
    :param max_span_days:
    :param group_by: Col. to bin counts by
    :param date_bin: size of date bins when grouping by date column
    :param days: custom size of bins when grouping by 'day'
    :param q: Filter and count only matching variants. May filter against samples as well.
    :param change_bin: When grouping by date, further bin by NT or AA? default AA.
    :return:
    """

    if group_by in SIMPLE_DATE_FIELDS:
        raise HTTPException(501, detail="This functionality has been removed for lack of use.")
    elif group_by == COLLECTION_DATE:
        return await DB.queries.counts.count_variants_by_collection_date(
            date_bin,
            change_bin,
            days,
            max_span_days,
            q
        )
    else:
        return await DB.queries.counts.count_variants_by_column(group_by)


@app.get('/v0/variants:freqByCollectionDate', response_model=List[Dict], tags=[TAG_VARIANTS], summary='Get variant alt-frequency quartiles by collection date')
async def get_aa_variant_frequency_by_collection_date(
    date_bin: DateBinOpt = DateBinOpt.month,
    days: int = DEFAULT_DAYS,
    q: str | None = None,
    max_span_days: int = DEFAULT_MAX_SPAN_DAYS
):
    return await DB.queries.variants.get_aa_variant_frequency_by_collection_date(
        date_bin,
        days,
        max_span_days,
        q
    )


@app.get('/v0/mutations:count', response_model=Dict[str, Dict[str, int]] | Dict[str, int], tags=[TAG_MUTATIONS], summary='Count mutations grouped by field or collection date')
async def get_mutation_counts(
    group_by: Annotated[str, Query(regex=WORDLIKE_PATTERN.pattern)],
    date_bin: DateBinOpt = DateBinOpt.month,
    days: int = DEFAULT_DAYS,
    q: str | None = None,
    change_bin: NtOrAa = NtOrAa.aa,
    max_span_days: int = DEFAULT_MAX_SPAN_DAYS
):
    if group_by in SIMPLE_DATE_FIELDS:
        raise HTTPException(501, detail="This functionality has been removed for lack of use.")
    elif group_by == COLLECTION_DATE:
        return await DB.queries.counts.count_mutations_by_collection_date(
            date_bin,
            change_bin,
            days,
            max_span_days,
            q
        )
    else:
        return await DB.queries.counts.count_mutations_by_column(group_by)


@app.get('/v0/mutations:countByCollectionDateAndLineage', response_model=List[Dict], tags=[TAG_MUTATIONS], summary='Count samples with a specific mutation by collection date and lineage')
async def get_aa_variant_frequency_by_collection_date(
    change_bin: NtOrAa,
    position: int,
    alt: str,
    region: str,
    date_bin: DateBinOpt = DateBinOpt.month,
    days: int = DEFAULT_DAYS,
    q: str | None = None,
    max_span_days: int = DEFAULT_MAX_SPAN_DAYS
):
    if change_bin == NtOrAa.nt:
        return await DB.queries.mutations.get_nt_mutation_count_by_collection_date(
            date_bin,
            position,
            alt,
            region,
            days,
            max_span_days,
            q
        )
    else:
        return await DB.queries.mutations.get_aa_mutation_count_by_collection_date(
            date_bin,
            position,
            alt,
            region,
            days,
            max_span_days,
            q
        )


# todo: I'm not crazy about this name.
#  We're not counting lineages here, we're counting how often they show up
#  Maybe this should be moved to
#  /v0/samples:count?group_by=lineage,release_date
# deprecated: use /v0/samples:count?group_by=lineage[,<date>]
@app.get('/v0/lineages:count', response_model=Dict[str, Dict[str, Dict[str, int]]] | List[LineageCountInfo], tags=[TAG_LINEAGES], summary='Count samples per lineage, optionally binned by date', deprecated=True)
async def get_lineage_counts(
    group_by: Annotated[str, Query(regex=WORDLIKE_PATTERN.pattern)] | None = None,
    date_bin: DateBinOpt = DateBinOpt.month,
    days: int = DEFAULT_DAYS,
    q: str | None = None,
    max_span_days: int = DEFAULT_MAX_SPAN_DAYS,
):
    if group_by in SIMPLE_DATE_FIELDS:
        return await DB.queries.counts.count_lineages_by_simple_date(group_by, date_bin, q, days)
    elif group_by == COLLECTION_DATE:
        return await DB.queries.counts.count_lineages_by_collection_date(date_bin, q, days, max_span_days)
    else:
        return await DB.queries.lineages.get_sample_counts_by_lineage(q)


@app.get(
    '/v0/lineages:abundance',
    response_model=Dict[str, List[LineageAbundanceSummaryInfo]]
                   | List[LineageAbundanceInfo]
                   | List[LineageAbundanceSummaryInfo],
    tags=[TAG_LINEAGES],
    summary='Get lineage abundances or abundance summary stats'
)
async def get_lineage_abundance(
    group_by: Annotated[str, Query(regex=WORDLIKE_PATTERN.pattern)] | None = None,
    date_bin: DateBinOpt = DateBinOpt.month,
    days: int = DEFAULT_DAYS,
    q: str | None = None,
    summary: bool = True,
    max_span_days: int = DEFAULT_MAX_SPAN_DAYS,
):
    if group_by in SIMPLE_DATE_FIELDS:
        if summary:
            return await DB.queries.lineages.get_abundance_summaries_by_simple_date(group_by, q, date_bin, days)
        else:
            raise HTTPException(status_code=501, detail='Not implemented, use summary results')  # Not implemented
    elif group_by == COLLECTION_DATE:
        if summary:
            return await DB.queries.lineages.get_abundance_summaries_by_collection_date(
                date_bin,
                days,
                q,
                max_span_days
            )
        else:
            raise HTTPException(status_code=501, detail='Not implemented, use summary results')  # Not implemented

    else:
        if summary:
            return await DB.queries.lineages.get_abundance_summaries(q)
        else:
            return await DB.queries.lineages.get_abundances(q)


@app.get('/v0/lineages:mutationIncidence', tags=[TAG_LINEAGES], summary='Get mutations prevalent within a lineage above a threshold')
async def get_mutation_incidence(
    lineage: str,
    lineage_system_name: str,
    change_bin: NtOrAa,
    prevalence_threshold: float = DEFAULT_PREVALENCE_THRESHOLD,
    match_reference: bool = False,
    q: str = None
):
    if prevalence_threshold < MIN_PREVALENCE_THRESHOLD:
        raise HTTPException(400, f'minimum allowed prevalence threshold is {MIN_PREVALENCE_THRESHOLD}')

    try:
        return await DB.queries.lineages.get_mutation_incidence(
            lineage,
            lineage_system_name,
            change_bin,
            prevalence_threshold,
            match_reference,
            q
        )
    except ParsingError as e:
        raise HTTPException(status_code=400, detail=e.message)


@app.get('/v0/lineages:mutationProfile', response_model=List[MutationProfileInfo], tags=[TAG_LINEAGES], summary="Get a lineage's nucleotide mutation spectrum")
async def get_mutation_profile(lineage: str, lineage_system_name: str, q: str = None) -> List[MutationProfileInfo]:
    return await DB.queries.lineages.get_mutation_profile(lineage, lineage_system_name, q)


@app.get('/variants:mutationLag', response_model=Dict[str, List[VariantMutationLagInfo]], tags=[TAG_VARIANTS], summary='Get changes seen as variants before becoming consensus mutations')
async def get_variants_before_mutations(lineage: str, lineage_system_name: str) -> List[VariantMutationLagInfo]:
    try:
        return await DB.queries.variants_mutations.get_variants_before_mutations(lineage, lineage_system_name)
    except ParsingError as e:
        raise HTTPException(status_code=400, detail=e.message)


@app.get('/variants:regionAndGffFeature', response_model=List[RegionAndGffFeatureInfo], tags=[TAG_VARIANTS], summary='List region and GFF-feature pairs present in variants')
async def get_region_and_gff_features() -> List[RegionAndGffFeatureInfo]:
    try:
        return await DB.queries.helpers.get_region_and_gff_features(IntraHostVariant)
    except ParsingError as e:
        raise HTTPException(status_code=400, detail=e.message)


@app.get('/mutations:variantLag', response_model=Dict[str, List[VariantMutationLagInfo]], tags=[TAG_MUTATIONS], summary='Get changes seen as consensus mutations before intra-host variants')
async def get_variants_before_mutations(lineage: str, lineage_system_name: str) -> List[VariantMutationLagInfo]:
    try:
        return await DB.queries.variants_mutations.get_mutations_before_variants(lineage, lineage_system_name)
    except ParsingError as e:
        raise HTTPException(status_code=400, detail=e.message)


@app.get('/mutations:gffFeature', response_model=List[str], tags=[TAG_MUTATIONS], summary='List region and GFF-feature pairs present in mutations')
async def get_gff_features() -> List[str]:
    try:
        gff_features = await DB.queries.helpers.get_gff_features()
        return gff_features
    except ParsingError as e:
        raise HTTPException(status_code=400, detail=e.message)


@app.get('/mutations:regionAndGffFeature', response_model=List[RegionAndGffFeatureInfo], tags=[TAG_MUTATIONS], summary='List region and GFF-feature pairs present in mutations', deprecated=True)
async def get_region_and_gff_features() -> List[RegionAndGffFeatureInfo]:
    try:
        return await DB.queries.helpers.get_region_and_gff_features(Mutation)
    except ParsingError as e:
        raise HTTPException(status_code=400, detail=e.message)


@app.get('/v0/phenotype_metric_values:countMutationsByCollectionDate', response_model=List[Dict], tags=[TAG_PHENOTYPE], summary='Count mutations at/above a phenotype threshold by collection date')
async def get_phenotype_metric_counts(
    phenotype_metric_name: str,
    phenotype_metric_value_threshold: str,
    date_bin: DateBinOpt = DateBinOpt.month,
    days: int = DEFAULT_DAYS,
    q: str | None = None,
    max_span_days: int = DEFAULT_MAX_SPAN_DAYS
):
    return await DB.queries.phenotype_metrics.count_variants_or_mutations_gte_pheno_value_by_collection_date(
        date_bin,
        phenotype_metric_name,
        phenotype_metric_value_threshold,
        days,
        max_span_days,
        q,
        Mutation
    )


@app.get('/v0/phenotype_metric_values:countVariantsByCollectionDate', response_model=List[Dict], tags=[TAG_PHENOTYPE], summary='Count variants at/above a phenotype threshold by collection date')
async def get_phenotype_metric_counts(
    phenotype_metric_name: str,
    phenotype_metric_value_threshold: float,
    date_bin: DateBinOpt = DateBinOpt.month,
    days: int = DEFAULT_DAYS,
    q: str | None = None,
    max_span_days: int = DEFAULT_MAX_SPAN_DAYS
):
    return await DB.queries.phenotype_metrics.count_variants_or_mutations_gte_pheno_value_by_collection_date(
        date_bin,
        phenotype_metric_name,
        phenotype_metric_value_threshold,
        days,
        max_span_days,
        q,
        IntraHostVariant
    )


@app.get('/v0/phenotype_metric_values:forMutationsAggregateBySampleAndCollectionDate', response_model=List[Dict], tags=[TAG_PHENOTYPE], summary='Get per-sample summed mutation phenotype values, summarized by collection date')
async def get_phenotype_metric_counts(
    phenotype_metric_name: str,
    date_bin: DateBinOpt = DateBinOpt.month,
    days: int = DEFAULT_DAYS,
    q: str | None = None,
    max_span_days: int = DEFAULT_MAX_SPAN_DAYS
):
    return await DB.queries.phenotype_metrics.get_pheno_value_for_mutations_by_sample_and_collection_date(
        date_bin,
        phenotype_metric_name,
        days,
        max_span_days,
        q
    )


@app.get('/v0/phenotype_metric_values:forVariantsAggregateBySampleAndCollectionDate', response_model=List[Dict], tags=[TAG_PHENOTYPE], summary='Get per-sample summed variant phenotype values, summarized by collection date')
async def get_phenotype_metric_counts(
    phenotype_metric_name: str,
    date_bin: DateBinOpt = DateBinOpt.month,
    days: int = DEFAULT_DAYS,
    q: str | None = None,
    max_span_days: int = DEFAULT_MAX_SPAN_DAYS
):
    return await DB.queries.phenotype_metrics.get_pheno_value_for_variants_by_sample_and_collection_date(
        date_bin,
        phenotype_metric_name,
        days,
        max_span_days,
        q
    )


@app.get('/v0/phenotype_metric_values:byMutationsQuantile', response_model=Dict[str, float], tags=[TAG_PHENOTYPE], summary='Get a phenotype metric value at a quantile across mutations')
async def get_phenotype_metric_value_by_mutation_quantile(phenotype_metric_name: str, quantile: float) -> Dict[
    str, float]:
    return await DB.queries.phenotype_metrics.get_phenotype_metric_value_by_mutation_quantile(
        phenotype_metric_name,
        quantile
    )


@app.get('/v0/phenotype_metric_values:byVariantsQuantile', response_model=Dict[str, float], tags=[TAG_PHENOTYPE], summary='Get a phenotype metric value at a quantile across variants')
async def get_phenotype_metric_value_by_variant_quantile(phenotype_metric_name: str, quantile: float) -> Dict[
    str, float]:
    return await DB.queries.phenotype_metrics.get_phenotype_metric_value_by_variant_quantile(
        phenotype_metric_name,
        quantile
    )


@app.get('/v0/phenotype_metric_values:getMinAndMaxValues', response_model=List, tags=[TAG_PHENOTYPE], summary='Get the min and max values of a phenotype metric')
async def get_phenotype_metric_value_min_and_max(phenotype_metric_name: str) -> List:
    return await DB.queries.phenotype_metrics.get_min_max_pheno_metric_value(phenotype_metric_name)


@app.get('/v0/annotations:byMutationsAndCollectionDate', response_model=List[Dict], tags=[TAG_ANNOTATIONS], summary='Get the proportion of mutations with an annotation effect by collection date')
async def get_annotations_by_mutations_and_collection_date(
    effect_detail: str,
    date_bin: DateBinOpt = DateBinOpt.month,
    days: int = DEFAULT_DAYS,
    max_span_days: int = DEFAULT_MAX_SPAN_DAYS,
    q: str | None = None
) -> List[Dict]:
    return await DB.queries.annotations.get_annotations_by_mutations_and_collection_date(
        effect_detail,
        date_bin,
        days,
        max_span_days,
        q
    )


@app.get('/v0/annotations:byVariantsAndCollectionDate', response_model=List[Dict], tags=[TAG_ANNOTATIONS], summary='Get the proportion of variants with an annotation effect by collection date')
async def get_annotations_by_variants_and_collection_date(
    effect_detail: str,
    date_bin: DateBinOpt = DateBinOpt.month,
    days: int = DEFAULT_DAYS,
    max_span_days: int = DEFAULT_MAX_SPAN_DAYS,
    q: str | None = None
) -> List[Dict]:
    return await DB.queries.annotations.get_annotations_by_variants_and_collection_date(
        effect_detail,
        date_bin,
        days,
        max_span_days,
        q
    )


@app.get('/v0/annotationEffects', response_model=List[str], tags=[TAG_ANNOTATIONS], summary='List all annotation effect types')
async def get_annotations_by_mutations_and_collection_date() -> List[str]:
    return await DB.queries.annotations.get_all_annotation_effects()


@app.get('/v0/annotations:byVariantsAndAminoAcidPosition', response_model=Dict, tags=[TAG_ANNOTATIONS], summary='Get annotated variant positions for an effect')
async def get_annotations_by_variants_and_amino_acid_position(
    effect_detail: str,
    q: str | None = None
) -> Dict:
    return await DB.queries.annotations.get_annotations_by_variants_and_amino_acid_position(effect_detail, q)


@app.get('/v0/annotations:byMutationsAndAminoAcidPosition', response_model=Dict, tags=[TAG_ANNOTATIONS], summary='Get annotated mutation positions for an effect')
async def get_annotations_by_mutations_and_amino_acid_position(
    effect_detail: str,
    q: str | None = None
) -> Dict:
    return await DB.queries.annotations.get_annotations_by_mutations_and_amino_acid_position(effect_detail, q)
