from typing import List, Any

from fastapi import FastAPI, HTTPException

import DB.queries.amino_acid_substitutions
import DB.queries.variants
from api.models import PydAminoAcidSubstitution, VariantInfo

app = FastAPI()


@app.get("/aa_subs/", response_model=list[PydAminoAcidSubstitution])
def get_aa_subs(sample_accession: str) -> Any:
    if sample_accession is None:
        raise HTTPException(status_code=400, detail='Must provide sample_accession')

    return DB.queries.amino_acid_substitutions.get_aa_subs_via_mutation_by_sample_accession(sample_accession)


# Trying out a shift in naming convention
# mutations = all data relevant to pop-level mutations
# variants = all data relevant to intra-host variants
@app.get('/variants/by/sample/{query}', response_model=List[VariantInfo])
def get_variants(query: str):
    if query is None:
        raise HTTPException(status_code=400, detail='Must provide query')

    return DB.queries.variants.get_variants_for_sample(query)