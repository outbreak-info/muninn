import re
from typing import List

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy.exc import ProgrammingError

import DB.queries.counts
import DB.queries.mutations
import DB.queries.samples
import DB.queries.variants
from api.models import VariantInfo, SampleInfo, MutationInfo

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=['*'],
    allow_credentials=False,
    allow_methods=['*'],
    allow_headers=['*']
)


# Trying out a shift in naming convention
# mutations = all data relevant to pop-level mutations
# variants = all data relevant to intra-host variants
@app.get('/variants/by/sample/{query}', response_model=List[VariantInfo])
def get_variants_by_sample(query: str):
    if query is None:
        raise HTTPException(status_code=400, detail='Must provide query')

    return DB.queries.variants.get_variants_for_sample(query)


@app.get('/mutations/by/sample/{query}', response_model=List[MutationInfo])
def get_mutations_by_sample(query: str):
    if query is None:
        raise HTTPException(status_code=400, detail='Must provide query')

    return DB.queries.mutations.get_mutations_by_sample(query)


# here I'm going to loosen things up a bit and apply the user's query to the results of a join
# between aas and alleles, then grab the samples via mutation.
@app.get('/samples/by/mutation/{query}', response_model=List[SampleInfo])
def get_samples_by_mutation(query: str):
    if query is None:
        raise HTTPException(status_code=400, detail='Must provide query')

    return DB.queries.samples.get_samples_by_mutation(query)


@app.get('/samples/by/variant/{query}', response_model=List[SampleInfo])
def get_samples_by_variant(query: str):
    if query is None:
        raise HTTPException(status_code=400, detail='Must provide query')

    return DB.queries.samples.get_samples_by_variant(query)


@app.get('/count/{x}/by/{y}', response_model=List[tuple])
def get_count_x_by_y(x: str, y: str):
    if x is None or y is None:
        raise HTTPException(status_code=400, detail='Provide target table and by-column')

    # todo: weak validation?
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
                raise HTTPException(status_code=400, detail=f'counts are available for: samples, variants, mutations')
    except ProgrammingError as e:
        # todo: logging
        short_message = str(e).split('\n')[0]
        raise HTTPException(status_code=400, detail=short_message)
