import re
from typing import List, Annotated

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
from utils.constants import CHANGE_PATTERN
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


@app.get('/count/{x}/by/{y}', response_model=List[tuple])
async def get_count_x_by_y(x: str, y: str):
    if x is None or y is None:
        raise HTTPException(status_code=400, detail='Provide target table and by-column')

    col_pattern = re.compile(r'\w+')
    if not col_pattern.fullmatch(y):
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
async def get_variant_counts_by_phenotype_score(region: str, metric: str, include_refs: bool = False, q: str | None = None):
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
async def get_mutation_counts_by_phenotype_score(region: str, metric: str, include_refs: bool = False, q: str | None = None):
    """
    :param region: Results will include only mutations in the given region
    :param metric: Phenotype metric whose values will be included in results
    :param include_refs: if true, include mutations where ref aa = alt aa
    :param q: Query against samples. If provided, only samples matching this query will be included in the count
    """
    return await DB.queries.prevalence.get_pheno_values_and_mutation_counts(metric, region, include_refs, q)


@app.get('/count/samples/lineages', response_model=List[LineageCountInfo])
def get_sample_counts_per_lineage(q: str | None = None):
    """
    :param q: A query to be run against samples. If provided, only samples matching the query will be counted.
    """
    try:
        return DB.queries.lineages.get_sample_counts_by_lineage(q)
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
