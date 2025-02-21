from typing import Any, List

import DB.models
from pydantic import BaseModel


# todo: Pyd* naming convention here is just a placeholder

class PydSample(BaseModel):
    id: int
    accession: str
    consent_level: str


class PydAllele(BaseModel):
    id: int
    region: str
    position_nt: int
    alt_nt: str


class PydAminoAcidSubstitution(BaseModel):
    id: int
    allele_id: int
    position_aa: int
    ref_aa: str
    alt_aa: str
    gff_feature: str


class PydIntraHostVariant(BaseModel):
    id: int
    sample_id: int
    allele_id: int
    ref_dp: int
    alt_dp: int
    alt_freq: float


# rm so you've queried for variants based on something, what do you want to know?
class VariantInfo(BaseModel):

    # todo: is there any reason to give the id?
    id: int
    sample_id: int
    allele_id: int
    ref_dp: int
    alt_dp: int
    alt_freq: float

    # Include allele info
    region: str
    position_nt: int
    ref_nt: str
    alt_nt: str

    amino_acid_mutations: List['PydAminoAcidSubstitution']


class VariantsForSample(BaseModel):
    sample: PydSample
    variants: List['VariantInfo']