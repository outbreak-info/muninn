import re
from typing import List, Annotated

from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy.exc import ProgrammingError

import DB.queries.counts
import DB.queries.mutations
import DB.queries.phenotype_metrics
import DB.queries.prevalence
import DB.queries.samples
import DB.queries.variants
from api.models import VariantInfo, SampleInfo, MutationInfo, VariantFreqInfo, VariantCountPhenoScoreInfo, \
    MutationCountInfo, PhenotypeMetricInfo
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
def get_sample_by_id(sample_id: int):
    sample = DB.queries.samples.get_sample_by_id(sample_id)
    if sample is None:
        raise HTTPException(status_code=404)
    return sample


@app.get('/samples', response_model=List[SampleInfo])
def get_samples_query(q: str):
    try:
        return DB.queries.samples.get_samples(q)
    except ParsingError as e:
        raise HTTPException(status_code=400, detail=e.message)


@app.get('/variants', response_model=List[VariantInfo])
def get_variants_query(q: str):
    try:
        return DB.queries.variants.get_variants(q)
    except ParsingError as e:
        raise HTTPException(status_code=400, detail=e.message)


@app.get('/mutations', response_model=List[MutationInfo])
def get_mutations_query(q: str):
    try:
        return DB.queries.mutations.get_mutations(q)
    except ParsingError as e:
        raise HTTPException(status_code=400, detail=e.message)


@app.get('/variants/by/sample', response_model=List[VariantInfo])
def get_variants_by_sample(q: str):
    try:
        return DB.queries.variants.get_variants_for_sample(q)
    except ParsingError as e:
        raise HTTPException(status_code=400, detail=e.message)


@app.get('/mutations/by/sample', response_model=List[MutationInfo])
def get_mutations_by_sample(q: str):
    try:
        return DB.queries.mutations.get_mutations_by_sample(q)
    except ParsingError as e:
        raise HTTPException(status_code=400, detail=e.message)


@app.get('/samples/by/mutation', response_model=List[SampleInfo])
def get_samples_by_mutation(q: str):
    try:
        return DB.queries.samples.get_samples_by_mutation(q)
    except ParsingError as e:
        raise HTTPException(status_code=400, detail=e.message)


@app.get('/samples/by/variant', response_model=List[SampleInfo])
def get_samples_by_variant(q: str):
    try:
        return DB.queries.samples.get_samples_by_variant(q)
    except ParsingError as e:
        raise HTTPException(status_code=400, detail=e.message)


@app.get('/count/{x}/by/{y}', response_model=List[tuple])
def get_count_x_by_y(x: str, y: str):
    if x is None or y is None:
        raise HTTPException(status_code=400, detail='Provide target table and by-column')

    col_pattern = re.compile(r'\w+')
    if not col_pattern.fullmatch(y):
        raise HTTPException(status_code=400, detail=f'This alleged column name fails validation: {y}')

    try:
        match x:
            case 'samples':
                return DB.queries.counts.count_samples_by_column(y)
            case 'variants':
                return DB.queries.counts.count_variants_by_column(y)
            case 'mutations':
                return DB.queries.counts.count_mutations_by_column(y)
            case _:
                raise HTTPException(status_code=400, detail='counts are available for: samples, variants, mutations')
    except ProgrammingError as e:
        # todo: logging
        short_message = str(e).split('\n')[0]
        raise HTTPException(status_code=400, detail=short_message)


@app.get('/variants/frequency', response_model=List[VariantFreqInfo])
def get_variant_frequency(
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
        return DB.queries.prevalence.get_samples_variant_freq_by_aa_change(aa)
    elif nt is not None:
        return DB.queries.prevalence.get_samples_variant_freq_by_nt_change(nt)


@app.get('/mutations/frequency', response_model=List[MutationCountInfo])
def get_mutation_sample_count(
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
        return DB.queries.prevalence.get_mutation_sample_count_by_aa(aa)
    elif nt is not None:
        return DB.queries.prevalence.get_mutation_sample_count_by_nt(nt)


@app.get('/variants/frequency/score', response_model=List[VariantCountPhenoScoreInfo])
def get_variant_counts_by_phenotype_score(region: str, metric: str, include_refs: bool = False):
    return DB.queries.prevalence.get_pheno_values_and_variant_counts(metric, region, include_refs)


@app.get('/mutations/frequency/score', response_model=List[VariantCountPhenoScoreInfo])
def get_mutation_counts_by_phenotype_score(region: str, metric: str, include_refs: bool = False):
    return DB.queries.prevalence.get_pheno_values_and_mutation_counts(metric, region, include_refs)


@app.get('/phenotype_metrics', response_model=List[PhenotypeMetricInfo])
def get_all_phenotype_metrics():
    return DB.queries.phenotype_metrics.get_all_pheno_metrics()
