from datetime import date, datetime
from typing import Any, List, Optional

from DB.models import Sample, AminoAcidSubstitution
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

    @classmethod
    def from_db_object(cls, dbo: 'AminoAcidSubstitution') -> 'PydAminoAcidSubstitution':
        return PydAminoAcidSubstitution(
            id=dbo.id,
            allele_id=dbo.allele_id,
            position_aa=dbo.position_aa,
            ref_aa=dbo.ref_aa,
            alt_aa=dbo.alt_aa,
            gff_feature=dbo.gff_feature
        )


class PydIntraHostVariant(BaseModel):
    id: int
    sample_id: int
    allele_id: int
    ref_dp: int
    alt_dp: int
    alt_freq: float


class VariantInfo(BaseModel):
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

    @classmethod
    def from_db_object(cls, dbo: 'Sample') -> 'SampleInfo':
       return SampleInfo(
            id=dbo.id,
            accession=dbo.accession,
            consent_level=dbo.consent_level,
            bio_project=dbo.bio_project,
            bio_sample=dbo.bio_sample,
            bio_sample_accession=dbo.bio_sample_accession,
            bio_sample_model=dbo.bio_sample_model,
            center_name=dbo.center_name,
            experiment=dbo.experiment,
            host=dbo.host,
            instrument=dbo.instrument,
            platform=dbo.platform,
            isolate=dbo.isolate,
            library_name=dbo.library_name,
            library_layout=dbo.library_layout,
            library_selection=dbo.library_selection,
            library_source=dbo.library_source,
            organism=dbo.organism,
            is_retracted=dbo.is_retracted,
            retraction_detected_date=dbo.retraction_detected_date,
            isolation_source=dbo.isolation_source,
            release_date=dbo.release_date,
            creation_date=dbo.creation_date,
            version=dbo.version,
            sample_name=dbo.sample_name,
            sra_study=dbo.sra_study,
            serotype=dbo.serotype,
            assay_type=dbo.assay_type,
            avg_spot_length=dbo.avg_spot_length,
            bases=dbo.bases,
            bytes=dbo.bytes,
            datastore_filetype=dbo.datastore_filetype,
            datastore_region=dbo.datastore_region,
            datastore_provider=dbo.datastore_provider,
            collection_start_date=dbo.collection_start_date,
            collection_end_date=dbo.collection_end_date,
            geo_location_id=dbo.geo_location_id,
            geo_country_name=dbo.r_geo_location.country_name,
            geo_region_name=dbo.r_geo_location.region_name,
            geo_locality_name=dbo.r_geo_location.locality_name
        )


class MutationInfo(BaseModel):
    id: int
    sample_id: int
    allele_id: int

    # include allele info
    region: str
    position_nt: int
    ref_nt: str
    alt_nt: str

    amino_acid_mutations: List['PydAminoAcidSubstitution']