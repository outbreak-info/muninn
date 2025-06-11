from typing import List, Annotated, Dict

from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy.exc import ProgrammingError

import DB.queries.counts
import DB.queries.lineages
import DB.queries.mutations
import DB.queries.phenotype_metrics
import DB.queries.prevalence
import DB.queries.samples
import DB.queries.variants
from api.models import VariantInfo, SampleInfo, MutationInfo, VariantFreqInfo, VariantCountPhenoScoreInfo, \
    MutationCountInfo, PhenotypeMetricInfo, LineageCountInfo, LineageAbundanceInfo, LineageAbundanceSummaryInfo
from utils.constants import CHANGE_PATTERN, WORDLIKE_PATTERN, DateBinOpt, SIMPLE_DATE_FIELDS, NtOrAa, \
    DEFAULT_MAX_SPAN_DAYS, COLLECTION_DATE, DEFAULT_DAYS, COMMA_SEP_WORDLIKE_PATTERN, LINEAGE
from utils.errors import ParsingError

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=['*'],
    allow_credentials=False,
    allow_methods=['*'],
    allow_headers=['*']
)


@app.get('/sample/{sample_id}', response_model=SampleInfo)
async def get_sample_by_id(sample_id: int):
    sample = await DB.queries.samples.get_sample_by_id(sample_id)
    if sample is None:
        raise HTTPException(status_code=404)
    return sample


@app.get('/phenotype_metrics', response_model=List[PhenotypeMetricInfo])
async def get_all_phenotype_metrics():
    return await DB.queries.phenotype_metrics.get_all_pheno_metrics()


@app.get('/samples', response_model=List[SampleInfo])
async def get_samples_query(q: str):
    try:
        return await DB.queries.samples.get_samples(q)
    except ParsingError as e:
        raise HTTPException(status_code=400, detail=e.message)


@app.get('/variants', response_model=List[VariantInfo])
async def get_variants_query(q: str):
    try:
        return await DB.queries.variants.get_variants(q)
    except ParsingError as e:
        raise HTTPException(status_code=400, detail=e.message)


@app.get('/mutations', response_model=List[MutationInfo])
async def get_mutations_query(q: str):
    try:
        return await DB.queries.mutations.get_mutations(q)
    except ParsingError as e:
        raise HTTPException(status_code=400, detail=e.message)


@app.get('/variants/by/sample', response_model=List[VariantInfo])
async def get_variants_by_sample(q: str):
    try:
        return await DB.queries.variants.get_variants_for_sample(q)
    except ParsingError as e:
        raise HTTPException(status_code=400, detail=e.message)


@app.get('/mutations/by/sample', response_model=List[MutationInfo])
async def get_mutations_by_sample(q: str):
    try:
        return await DB.queries.mutations.get_mutations_by_sample(q)
    except ParsingError as e:
        raise HTTPException(status_code=400, detail=e.message)


@app.get('/samples/by/mutation', response_model=List[SampleInfo])
async def get_samples_by_mutation(q: str):
    try:
        return await DB.queries.samples.get_samples_by_mutation(q)
    except ParsingError as e:
        raise HTTPException(status_code=400, detail=e.message)


@app.get('/samples/by/variant', response_model=List[SampleInfo])
async def get_samples_by_variant(q: str):
    try:
        return await DB.queries.samples.get_samples_by_variant(q)
    except ParsingError as e:
        raise HTTPException(status_code=400, detail=e.message)


@app.get('/count/{x}/by/{y}', response_model=Dict[str, int])
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
        # todo: logging
        short_message = str(e).split('\n')[0]
        raise HTTPException(status_code=400, detail=short_message)


@app.get('/variants/frequency', response_model=List[VariantFreqInfo])
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
@app.get('/mutations/frequency', response_model=List[MutationCountInfo])
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
@app.get('/variants/frequency/score', response_model=List[VariantCountPhenoScoreInfo])
async def get_variant_counts_by_phenotype_score(
    region: str,
    metric: str,
    include_refs: bool = False,
    q: str | None = None
):
    """
    :param region: Results will include only variants in the given region
    :param metric: Phenotype metric whose values will be included in results
    :param include_refs: if true, include variants where ref aa = alt aa
    :param q: Query against samples. If provided, only samples matching this query will be included in the count
    """
    return await DB.queries.prevalence.get_pheno_values_and_variant_counts(metric, region, include_refs, q)


# todo: actually a count
#  /count/samples/pheno_scores/mutations
@app.get('/mutations/frequency/score', response_model=List[VariantCountPhenoScoreInfo])
async def get_mutation_counts_by_phenotype_score(
    region: str,
    metric: str,
    include_refs: bool = False,
    q: str | None = None
):
    """
    :param region: Results will include only mutations in the given region
    :param metric: Phenotype metric whose values will be included in results
    :param include_refs: if true, include mutations where ref aa = alt aa
    :param q: Query against samples. If provided, only samples matching this query will be included in the count
    """
    return await DB.queries.prevalence.get_pheno_values_and_mutation_counts(metric, region, include_refs, q)


@app.get('/count/samples/lineages', response_model=List[LineageCountInfo])
async def get_sample_counts_per_lineage(q: str | None = None):
    """
    :param q: A query to be run against samples. If provided, only samples matching the query will be counted.
    """
    try:
        return await DB.queries.lineages.get_sample_counts_by_lineage(q)
    except ParsingError as e:
        raise HTTPException(status_code=400, detail=e.message)


@app.get('/lineages/abundances', response_model=List[LineageAbundanceInfo])
async def get_lineage_abundance_info(q: str | None = None):
    """
    :param q: a query to be run against lineages and samples.
    Note that results without abundance numbers are always excluded.
    """
    try:
        return await DB.queries.lineages.get_abundances(raw_query=q)
    except ParsingError as e:
        raise HTTPException(status_code=400, detail=e.message)


@app.get('/lineages/abundances/summary_stats', response_model=List[LineageAbundanceSummaryInfo])
async def get_lineage_abundance_summary_stats(q: str | None = None):
    try:
        return await DB.queries.lineages.get_abundance_summaries(q)
    except ParsingError as e:
        raise HTTPException(status_code=400, detail=e.message)


@app.get(
    '/v0/samples:count',
    response_model=Dict[str, int] | Dict[str, Dict[str, Dict[str, int]]] | List[LineageCountInfo]
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


@app.get('/v0/variants:count', response_model=Dict[str, Dict[str, int]] | Dict[str, int])
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
        return await DB.queries.counts.count_variants_by_simple_date(group_by, date_bin, days, q, change_bin)
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


@app.get('/v0/mutations:count', response_model=Dict[str, Dict[str, int]] | Dict[str, int])
async def get_mutation_counts(
    group_by: Annotated[str, Query(regex=WORDLIKE_PATTERN.pattern)],
    date_bin: DateBinOpt = DateBinOpt.month,
    days: int = DEFAULT_DAYS,
    q: str | None = None,
    change_bin: NtOrAa = NtOrAa.aa,
    max_span_days: int = DEFAULT_MAX_SPAN_DAYS
):
    if group_by in SIMPLE_DATE_FIELDS:
        return await DB.queries.counts.count_mutations_by_simple_date(group_by, date_bin, days, q, change_bin)
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


# todo: I'm not crazy about this name.
#  We're not counting lineages here, we're counting how often they show up
#  Maybe this should be moved to
#  /v0/samples:count?group_by=lineage,release_date
@app.get('/v0/lineages:count', response_model=Dict[str, Dict[str, Dict[str, int]]] | List[LineageCountInfo])
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
                   | List[LineageAbundanceSummaryInfo]
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

@app.get('/lineages/mutation_incidence')
async def get_mutation_incidence(lineage:str,change_bin:NtOrAa,q: str | None = None):
    return await DB.queries.lineages.get_mutation_incidence(lineage,change_bin,q)