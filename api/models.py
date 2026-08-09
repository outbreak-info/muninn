from datetime import date, datetime
from typing import List, Optional, Dict

from pydantic import BaseModel, Field

from DB.models import IntraHostVariant, Sample, AminoAcid, Mutation, MutationTranslation, \
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


class VariantNucleotideInfo(BaseModel):
    sample_id: int = Field(description="ID of the sample carrying this intra-host variant")
    allele_id: int = Field(description="ID of the allele (nt change) in the alleles table")

    # allele (nt) info
    region: str = Field(description="Genomic region / segment the allele falls in")
    position_nt: int = Field(description="1-based nucleotide position of the change within the region")
    ref_nt: str = Field(description="Reference nucleotide(s) at this position")
    alt_nt: str = Field(description="Alternate (variant) nucleotide(s) at this position")

    # intra-host variant metrics
    ref_dp: int = Field(description="Read depth supporting the reference allele")
    alt_dp: int = Field(description="Read depth supporting the alternate allele")
    alt_freq: float = Field(description="Intra-host alternate-allele frequency (alt_dp / total depth)")


class VariantAminoAcidInfo(BaseModel):
    sample_id: int = Field(description="ID of the sample carrying this intra-host amino-acid variant")
    position_aa: int = Field(description="1-based amino-acid position of the change within the feature")
    ref_aa: str = Field(description="Reference amino acid at this position")
    alt_aa: str = Field(description="Alternate (variant) amino acid at this position")
    gff_feature: str = Field(description="GFF feature (gene/product) the amino-acid change falls in")
    ref_codon: str = Field(description="Reference codon")
    alt_codon: str = Field(description="Alternate (variant) codon")


class SampleInfo(BaseModel):
    id: int = Field(description="Muninn internal sample ID (primary key of the samples table)")

    # columns from ncbi
    accession: str = Field(description="SRA run accession")
    bio_project: str | None = Field(description="BioProject accession")
    bio_sample: str | None = Field(description="BioSample ID")
    bio_sample_accession: str | None = Field(description="BioSample accession")
    bio_sample_model: str | None = Field(description="BioSample model")
    center_name: str | None = Field(description="Sequencing center name")
    experiment: str | None = Field(description="SRA experiment accession")
    host: str | None = Field(description="Host organism")
    instrument: str | None = Field(description="Sequencing instrument")
    platform: str | None = Field(description="Sequencing platform")
    isolate: str | None = Field(description="Isolate identifier")
    library_name: str | None = Field(description="Library name")
    library_layout: str | None = Field(description="Library layout, e.g. single/paired")
    library_selection: str | None = Field(description="Library selection method")
    library_source: str | None = Field(description="Library source")
    organism: str = Field(description="Organism name")
    is_retracted: bool = Field(description="Whether the record has been retracted")
    retraction_detected_date: datetime | None = Field(description="Date retraction was detected")
    isolation_source: str | None = Field(description="Isolation source")
    release_date: datetime | None = Field(description="SRA release date")
    creation_date: datetime | None = Field(description="SRA record creation date")
    version: str | None = Field(description="Record version")
    sample_name: str | None = Field(description="Sample name")
    sra_study: str | None = Field(description="SRA study accession")
    serotype: str | None = Field(description="Serotype")
    assay_type: str | None = Field(description="Assay type")
    avg_spot_length: float | None = Field(description="Average spot length")
    bases: int | None = Field(description="Total base count")
    collection_start_date: date | None = Field(description="Sample collection start date")
    collection_end_date: date | None = Field(description="Sample collection end date")
    geo_location_id: int | None = Field(description="Foreign key to the geo_locations table (derived from NCBI geographic location)")

    # geo data (from the joined geo_locations table)
    geo_country_name: str | None = Field(description="Country name")
    geo_admin1_name: str | None = Field(description="Admin level 1 name, e.g. state/province")
    geo_admin2_name: str | None = Field(description="Admin level 2 name, e.g. county ")
    geo_admin3_name: str | None = Field(description="Admin level 3 name")

    # wastewater-specific columns (from wastewater surveillance data, not NCBI)
    ww_viral_load: float | None = Field(description="Wastewater viral load")
    ww_catchment_population: int | None = Field(description="Catchment population served by the sampling site")
    ww_site_id: str | None = Field(description="Wastewater sampling site identifier")
    ww_collected_by: str | None = Field(description="Organization that collected the wastewater sample")


class MutationNucleotideInfo(BaseModel):
    sample_id: int = Field(description="ID of the sample carrying this consensus mutation")
    allele_id: int = Field(description="ID of the allele (nt change) in the alleles table")

    # allele (nt) info
    region: str = Field(description="Genomic region / segment the allele falls in")
    position_nt: int = Field(description="1-based nucleotide position of the change within the region")
    ref_nt: str = Field(description="Reference nucleotide(s) at this position")
    alt_nt: str = Field(description="Alternate (mutant) nucleotide(s) at this position")


class MutationAminoAcidInfo(BaseModel):
    sample_id: int = Field(description="ID of the sample carrying this consensus amino-acid mutation")
    position_aa: int = Field(description="1-based amino-acid position of the change within the feature")
    ref_aa: str = Field(description="Reference amino acid at this position")
    alt_aa: str = Field(description="Alternate (mutant) amino acid at this position")
    gff_feature: str = Field(description="GFF feature (gene/product) the amino-acid change falls in")
    ref_codon: str = Field(description="Reference codon")
    alt_codon: str = Field(description="Alternate (mutant) codon")


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
    id: int = Field(description="Muninn internal phenotype-metric ID (primary key of the phenotype_metrics table)")
    name: str = Field(description="Phenotype metric name (phenotype_metrics.phenotype_metric_name), e.g. an antibody/assay identifier")
    assay_type: str = Field(description="Assay type the metric was measured with (phenotype_metrics.phenotype_metric_assay_type)")


class VariantFreqInfo(BaseModel):
    alt_freq: float
    accession: str
    allele_id: int
    translation_id: int | None
    amino_sub_id: int | None


class MutationCountInfo(BaseModel):
    sample_count: int = Field(description="Number of samples carrying this consensus mutation")
    amino_sub_id: int | None = Field(description="amino_acids.id of the matched amino-acid change for aa lookups; null for nt lookups")


class VariantCountPhenoScoreInfo(BaseModel):
    ref_aa: str = Field(description="Reference amino acid at this position")
    alt_aa: str = Field(description="Alternate (mutant) amino acid at this position")
    position_aa: int = Field(description="1-based amino-acid position of the change within the GFF feature")
    pheno_value: float = Field(description="Value of the requested phenotype metric for this amino-acid change")
    count: int = Field(description="Number of distinct samples carrying this amino-acid change (after any sample filter)")


class LineageCountInfo(BaseModel):
    count: int = Field(description="Number of distinct samples assigned to this lineage")
    lineage_system: str | None = Field(description="Name of the lineage nomenclature system; null when the lineage has no associated system")
    lineage: str | None = Field(description="Lineage name; null when the count is not attributed to a named lineage")


class LineageCountWithPrevalenceInfo(LineageCountInfo):
    total: int = Field(description="Total number of samples in this collection-date bin across all lineages (denominator; not affected by the `lineage` filter).")
    prevalence: float = Field(description="Fraction of this bin's samples assigned to this lineage (count / total)")


class LineageInfo(BaseModel):
    lineage_id: int = Field(description="Lineage ID")
    lineage_name: str = Field(description="Lineage name within its nomenclature system (e.g. a Pango lineage like 'BA.2')")
    lineage_system_id: int = Field(description="Lineage nomenclature system ID")
    lineage_system_name: str = Field(description="Name of the lineage nomenclature system this lineage belongs to (e.g. 'PANGO')")


class LineageAbundanceInfo(BaseModel):
    lineage_info: 'LineageInfo' = Field(description="The lineage this abundance is for, and its nomenclature system")
    sample_id: int = Field(description="Sample ID this abundance was measured in")
    accession: str = Field(description="SRA run accession of the sample")
    abundance: float = Field(description="Relative abundance of the lineage in the sample (0-1), from abundance-based (non-consensus) lineage calls")


# wastewater-specific
class LineageAbundanceWithSampleInfo(BaseModel):
    accession: str
    admin1_name: str
    ww_collected_by: str | None
    ww_site_id: str
    lineage_name: str
    abundance: float
    ww_viral_load: float | None
    ww_catchment_population: int
    collection_start_date: date


# wastewater-specific
class AverageLineageAbundanceInfo(BaseModel):
    year: int
    chunk: int
    epiweek: int
    week_start: date
    week_end: date
    lineage_name: str
    census_region: str
    geo_admin1_name: str | None
    sample_count: int
    mean_viral_load: float | None
    mean_catchment_size: float
    mean_lineage_prevalence: float


class LineageAbundanceSummaryInfo(BaseModel):
    lineage_name: str = Field(description="Lineage name within its nomenclature system")
    lineage_system_name: str = Field(description="Name of the lineage nomenclature system (e.g. 'PANGO')")
    sample_count: int = Field(description="Number of (abundance-based) samples aggregated into this row")
    abundance_min: float = Field(description="Minimum lineage abundance across the aggregated samples (0-1)")
    abundance_q1: float = Field(description="First quartile (25th percentile) of lineage abundance across the aggregated samples")
    abundance_median: float = Field(description="Median (50th percentile) of lineage abundance across the aggregated samples")
    abundance_q3: float = Field(description="Third quartile (75th percentile) of lineage abundance across the aggregated samples")
    abundance_max: float = Field(description="Maximum lineage abundance across the aggregated samples (0-1)")


class VariantMutationLagInfo(BaseModel):
    variants_start_date: date = Field(description="Earliest sample collection-start date on which this amino-acid change was observed as an intra-host variant within the queried lineage")
    mutations_start_date: date = Field(description="Earliest sample collection-start date on which this amino-acid change was observed as a consensus mutation within the queried lineage")
    lag: int = Field(description="Number of days between the first-variant and first-mutation dates (always >= 0; which change type leads is fixed by the endpoint: :variantLag = mutation seen first, :mutationLag = variant seen first)")
    ref: str = Field(description="Reference amino acid of the change")
    pos: int = Field(description="1-based amino-acid position of the change within the GFF feature")
    alt: str = Field(description="Alternate (mutant) amino acid of the change")


class RegionAndGffFeatureInfo(BaseModel):
    gff_feature: str
    region: str


class MutationProfileInfo(BaseModel):
    ref_nt: str = Field(description="Reference nucleotide of the substitution (one of A/C/G/T)")
    alt_nt: str = Field(description="Alternate (mutated) nucleotide of the substitution (one of A/C/G/T)")
    region: str = Field(description="Genomic region / segment the substitution falls in")
    count: int = Field(description="Number of (sample, allele) occurrences of this ref→alt substitution class in the lineage's samples")


class MutationProfileWithPrevalenceInfo(MutationProfileInfo):
    total: int = Field(description="Total number of substitution occurrences in this region across all ref→alt classes (denominator)")
    prevalence: float = Field(description="Fraction of this region's substitutions that are this ref→alt class (count / total); prevalences within a region sum to 1")


class MutationIncidenceEntryInfo(BaseModel):
    ref: str = Field(description="Reference base (nt) or amino acid (aa) at this position")
    alt: str = Field(description="Alternate base (nt) or amino acid (aa) at this position")
    pos: int = Field(description="1-based position of the change (nucleotide position for nt, amino-acid position for aa)")
    count: int = Field(description="Number of samples in the lineage subset that carry this consensus mutation")
    prevalence: float = Field(description="Fraction of the lineage subset carrying this mutation (count / sample_count)")


class MutationIncidenceInfo(BaseModel):
    sample_count: int = Field(description="Number of samples in the requested lineage (after applying any filter)")
    mutation_counts: Dict[str, List[MutationIncidenceEntryInfo]] = Field(
        description="Mutations meeting the prevalence threshold, keyed by region (nt) or GFF feature (aa); "
                    "each value is the list of qualifying changes in that region/feature"
    )


class SampleCollectionReleaseLagInfo(BaseModel):
    collection_date_bin: str = Field(
        description="Date-bin label for the collection-window midpoint: e.g. '2024-06' (month), "
                    "'2024-W05' (week), or a 'start/end' interval (day)."
    )
    lag_q1: float | None = Field(
        description="First quartile (25th percentile) of the lag, in days, from the collection "
                    "midpoint to the release date. Null when the bin has no released samples."
    )
    lag_median: float | None = Field(
        description="Median (50th percentile) of the lag, in days, from the collection midpoint "
                    "to the release date. Null when the bin has no released samples."
    )
    lag_q3: float | None = Field(
        description="Third quartile (75th percentile) of the lag, in days, from the collection "
                    "midpoint to the release date. Null when the bin has no released samples."
    )


class VariantFrequencyByCollectionDateInfo(BaseModel):
    """
    Shared fields of a (date bin, change) frequency row. Not a response model on its own: the endpoint
    returns the nucleotide or amino-acid subclass, which is what identifies the change. Keep the
    subclasses' field names disjoint — the response model is a plain union, and pydantic picks the
    member by which required fields are present.
    """
    date: str = Field(
        description="Date-bin label for the collection-window midpoint: e.g. '2024-06' (month), "
                    "'2024-W05' (week), or a 'start/end' interval (day)."
    )
    n: int = Field(
        description="Number of distinct samples in this date bin carrying this change as an intra-host "
                    "variant"
    )
    alt_freq_q1: float = Field(
        description="First quartile (25th percentile) of the intra-host alternate-allele frequency of "
                    "this change across those samples. Frequency is stored as a 0.05-wide bin rather "
                    "than an exact value, so each sample contributes its bin's midpoint: read these "
                    "quartiles as accurate to about +/-0.025, and note that ingestion discards "
                    "intra-host calls below 0.2, so the distribution is truncated there."
    )
    alt_freq_median: float = Field(
        description="Median (50th percentile) of the binned intra-host alternate-allele frequency; see "
                    "alt_freq_q1 for the resolution caveat"
    )
    alt_freq_q3: float = Field(
        description="Third quartile (75th percentile) of the binned intra-host alternate-allele "
                    "frequency; see alt_freq_q1 for the resolution caveat"
    )


class VariantAminoAcidFrequencyByCollectionDateInfo(VariantFrequencyByCollectionDateInfo):
    gff_feature: str = Field(description="GFF feature (gene/product) the amino-acid change falls in")
    ref_aa: str = Field(description="Reference amino acid of the change")
    position_aa: int = Field(description="1-based amino-acid position of the change within the GFF feature")
    alt_aa: str = Field(description="Alternate (mutant) amino acid of the change")


class VariantNucleotideFrequencyByCollectionDateInfo(VariantFrequencyByCollectionDateInfo):
    region: str = Field(description="Genomic region / segment the nucleotide change falls in")
    ref_nt: str = Field(description="Reference nucleotide of the change")
    position_nt: int = Field(description="1-based nucleotide position of the change within the region")
    alt_nt: str = Field(description="Alternate (mutant) nucleotide of the change")
