from datetime import date, datetime
from typing import Any, List, Optional

import DB.models
from pydantic import BaseModel


# todo: Pyd* naming convention here is just a placeholder

class PydSample(BaseModel):
    id: int
    accession: str
    consent_level: str
    bio_project: str
    bio_sample: str
    bio_sample_accession: str
    bio_sample_model: str
    center_name: str
    experiment: str
    host: str
    instrument: str
    platform: str
    isolate: str
    library_name: str
    library_layout: str
    library_selection: str
    library_source: str
    organism: str
    is_retracted: bool
    retraction_detected_date: datetime
    isolation_source: str
    release_date: datetime
    creation_date: datetime
    version: str
    sample_name: str
    sra_study: str
    serotype: str
    assay_type: str
    avg_spot_length: float
    bases: int
    bytes: int
    datastore_filetype: str
    datastore_region: str
    datastore_provider: str
    collection_start_date: date
    collection_end_date: date
    geo_location_id: str


class PydGeoLocations(BaseModel):
    id: int
    full_text: str
    continent_name: str
    geo_country_name: str
    geo_region_name: str
    geo_locality_name: str

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


class SampleInfo(BaseModel):
    id: int
    accession: str
    consent_level: str
    bio_project: str
    bio_sample: Optional[str]
    bio_sample_accession: Optional[str]
    bio_sample_model: str
    center_name: str
    experiment: str
    host: Optional[str]
    instrument: str
    platform: str
    isolate: Optional[str]
    library_name: str
    library_layout: str
    library_selection: str
    library_source: str
    organism: str
    is_retracted: bool
    retraction_detected_date: Optional[datetime]
    isolation_source: Optional[str]
    release_date: datetime
    creation_date: datetime
    version: str
    sample_name: str
    sra_study: str
    serotype: str
    assay_type: str
    avg_spot_length: float
    bases: int
    bytes: int
    datastore_filetype: str
    datastore_region: str
    datastore_provider: str
    collection_start_date: Optional[date]
    collection_end_date: Optional[date]
    geo_location_id: Optional[int]

    # geo data
    geo_country_name: Optional[str]
    geo_region_name: Optional[str]
    geo_locality_name: Optional[str]
