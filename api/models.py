from datetime import date, datetime
from typing import List, Optional

from pydantic import BaseModel

from DB.models import IntraHostVariant, Sample, AminoAcidSubstitution, Mutation, PhenotypeMetric

"""
These models define the shapes for data returned by the api.
They correspond closely, but not exactly, to the ORM models.
In case it's not clear, the naming convention here is 'ThingInfo'.
"""


class AminoAcidSubInfo(BaseModel):
    id: int
    position_aa: int
    ref_aa: str
    alt_aa: str
    gff_feature: str

    @classmethod
    def from_db_object(cls, dbo: 'AminoAcidSubstitution') -> 'AminoAcidSubInfo':
        return AminoAcidSubInfo(
            id=dbo.id,
            position_aa=dbo.position_aa,
            ref_aa=dbo.ref_aa,
            alt_aa=dbo.alt_aa,
            gff_feature=dbo.gff_feature
        )


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

    amino_acid_mutations: List['AminoAcidSubInfo']

    @classmethod
    def from_db_object(cls, dbo: 'IntraHostVariant'):
        return VariantInfo(
            id=dbo.id,
            sample_id=dbo.sample_id,
            allele_id=dbo.allele_id,
            ref_dp=dbo.ref_dp,
            alt_dp=dbo.alt_dp,
            alt_freq=dbo.alt_freq,
            region=dbo.r_allele.region,
            position_nt=dbo.r_allele.position_nt,
            ref_nt=dbo.r_allele.ref_nt,
            alt_nt=dbo.r_allele.alt_nt,
            amino_acid_mutations=[AminoAcidSubInfo.from_db_object(t.r_amino_sub) for t in dbo.r_allele.r_translations]
        )


class SampleInfo(BaseModel):
    id: int
    accession: str
    consent_level: str
    bio_project: str
    bio_sample: str | None
    bio_sample_accession: str | None
    bio_sample_model: str
    center_name: str
    experiment: str
    host: str | None
    instrument: str
    platform: str
    isolate: str | None
    library_name: str
    library_layout: str
    library_selection: str
    library_source: str
    organism: str
    is_retracted: bool
    retraction_detected_date: datetime | None
    isolation_source: str | None
    release_date: datetime
    creation_date: datetime
    version: str
    sample_name: str
    sra_study: str
    serotype: str | None
    assay_type: str
    avg_spot_length: float | None
    bases: int
    bytes: int
    datastore_filetype: str
    datastore_region: str
    datastore_provider: str
    collection_start_date: date | None
    collection_end_date: date | None
    geo_location_id: int | None

    # geo data
    geo_country_name: str | None
    geo_region_name: str | None
    geo_locality_name: str | None

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

    amino_acid_mutations: List['AminoAcidSubInfo']

    @classmethod
    def from_db_object(cls, dbo: 'Mutation') -> 'MutationInfo':
        return MutationInfo(
            id=dbo.id,
            sample_id=dbo.sample_id,
            allele_id=dbo.allele_id,
            region=dbo.r_allele.region,
            position_nt=dbo.r_allele.position_nt,
            ref_nt=dbo.r_allele.ref_nt,
            alt_nt=dbo.r_allele.alt_nt,
            amino_acid_mutations=[AminoAcidSubInfo.from_db_object(t.r_amino_sub) for t in dbo.r_allele.r_translations]
        )

class PhenotypeMetricInfo(BaseModel):
    id: int
    name: str
    assay_type: str

    @classmethod
    def from_db_object(cls, dbo: 'PhenotypeMetric') -> 'PhenotypeMetricInfo':
        return PhenotypeMetricInfo(
            id=dbo.id,
            name=dbo.name,
            assay_type=dbo.assay_type
        )


class VariantFreqInfo(BaseModel):
    alt_freq: float
    accession: str
    allele_id: int
    translation_id: int | None
    amino_sub_id: int | None

class MutationCountInfo(BaseModel):
    sample_count: int
    allele_id: int
    translation_id: int | None
    amino_sub_id: int | None

class VariantCountPhenoScoreInfo(BaseModel):
    ref_aa: str
    alt_aa: str
    position_aa: int
    pheno_value: float
    count: int