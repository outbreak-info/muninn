from datetime import date, datetime
from typing import List, Optional

from pydantic import BaseModel

from DB.models import IntraHostVariant, Sample, AminoAcid, Mutation, PhenotypeMetric, MutationTranslation, \
    IntraHostTranslation

"""
These models define the shapes for data returned by the api.
They correspond closely, but not exactly, to the ORM models.
In case it's not clear, the naming convention here is 'ThingInfo'.
"""


class AminoAcidInfo(BaseModel):
    id: int
    position_aa: int
    ref_aa: str
    alt_aa: str
    gff_feature: str
    ref_codon: str
    alt_codon: str

    @classmethod
    def from_db_object(cls, dbo: MutationTranslation | IntraHostTranslation | None) -> Optional['AminoAcidInfo']:
        if dbo is None:
            return None
        # noinspection PyTypeChecker
        amino_acid: AminoAcid = dbo.r_amino_acid
        return AminoAcidInfo(
            id=amino_acid.id,
            position_aa=amino_acid.position_aa,
            ref_aa=amino_acid.ref_aa,
            alt_aa=amino_acid.alt_aa,
            gff_feature=amino_acid.gff_feature,
            ref_codon=amino_acid.ref_codon,
            alt_codon=amino_acid.alt_codon,
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

    amino_acid_mutations: List['AminoAcidInfo']

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
            amino_acid_mutations=[AminoAcidInfo.from_db_object(t) for t in dbo.r_translations]
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
    geo_admin1_name: str | None
    geo_admin2_name: str | None
    geo_admin3_name: str | None

    # wastewater-specific columns
    ww_viral_load: float | None
    ww_catchment_population: int | None
    ww_site_id: str | None
    ww_collected_by: str | None

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
            geo_admin1_name=dbo.r_geo_location.admin1_name,
            geo_admin2_name=dbo.r_geo_location.admin2_name,
            geo_admin3_name=dbo.r_geo_location.admin3_name,
            ww_viral_load=dbo.ww_viral_load,
            ww_catchment_population=dbo.ww_catchment_population,
            ww_site_id=dbo.ww_site_id,
            ww_collected_by=dbo.ww_collected_by,
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

    amino_acid_mutations: List[AminoAcidInfo]

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
            amino_acid_mutations=[AminoAcidInfo.from_db_object(t) for t in dbo.r_translations]
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


class LineageCountInfo(BaseModel):
    count: int
    lineage_system: str | None
    lineage: str | None


# todo: do I want to drop the ids?
class LineageInfo(BaseModel):
    lineage_id: int
    lineage_name: str
    lineage_system_id: int
    lineage_system_name: str


class LineageAbundanceInfo(BaseModel):
    lineage_info: 'LineageInfo'
    sample_id: int
    accession: str
    abundance: float

# wastewater-specific
class LineageAbundanceWithSampleInfo(BaseModel):
    accession: str
    ww_collected_by: str
    lineage_name: str
    abundance: float
    ww_viral_load: float
    collection_date: date

# wastewater-specific
class AverageLineageAbundanceInfo(BaseModel):
    year: int
    week: int
    epiweek: int
    week_start: date
    week_end: date
    lineage_name: str
    census_region: str
    state: str
    sample_count: int
    mean_viral_load: float
    mean_catchment_size: float
    mean_lineage_prevalence: float

class LineageAbundanceSummaryInfo(BaseModel):
    lineage_name: str
    lineage_system_name: str
    sample_count: int
    abundance_min: float
    abundance_q1: float
    abundance_median: float
    abundance_q3: float
    abundance_max: float


class VariantMutationLagInfo(BaseModel):
    variants_start_date: date
    mutations_start_date: date
    lag: int
    ref: str
    pos: int
    alt: str


class RegionAndGffFeatureInfo(BaseModel):
    gff_feature: str
    region: str


class MutationProfileInfo(BaseModel):
    ref_nt: str
    alt_nt: str
    region: str
    count: int
