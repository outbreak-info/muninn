import logging
from typing import List, Annotated, Dict

import re

from fastapi import APIRouter, FastAPI, HTTPException, Path, Query, Request
from fastapi.routing import APIRoute
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from fastmcp import FastMCP
from sqlalchemy.exc import DBAPIError

import DB.queries.annotations
import DB.queries.counts
import DB.queries.helpers
import DB.queries.lineages
import DB.queries.mutations
import DB.queries.phenotype_metrics
import DB.queries.prevalence
import DB.queries.samples
import DB.queries.variants
import DB.queries.variants_mutations
import DB.queries.wastewater
from api.models import VariantNucleotideInfo, VariantAminoAcidInfo, SampleInfo, \
    MutationNucleotideInfo, MutationAminoAcidInfo, \
    VariantCountPhenoScoreInfo, \
    MutationCountInfo, PhenotypeMetricInfo, LineageCountInfo, LineageAbundanceInfo, LineageAbundanceSummaryInfo, \
    LineageInfo, VariantMutationLagInfo, \
    LineageCountWithPrevalenceInfo, MutationProfileWithPrevalenceInfo, \
    SampleCollectionReleaseLagInfo, MutationIncidenceInfo, \
    VariantAminoAcidFrequencyByCollectionDateInfo, VariantNucleotideFrequencyByCollectionDateInfo, \
    VariantFreqInfo, MutationNucleotideCountByDateAndLineageInfo, \
    MutationAminoAcidCountByDateAndLineageInfo, PhenotypeMetricDateCountInfo, \
    PhenotypeMetricAggregateByDateInfo, AnnotationProportionByDateInfo, AnnotatedPositionCountInfo, \
    LineageAbundanceWithSampleInfo, AverageLineageAbundanceInfo
from utils.constants import CHANGE_PATTERN, WORDLIKE_PATTERN, DateBinOpt, NtOrAa, \
    DEFAULT_MAX_SPAN_DAYS, COLLECTION_DATE, DEFAULT_DAYS, COMMA_SEP_WORDLIKE_PATTERN, \
    DEFAULT_PREVALENCE_THRESHOLD, MIN_PREVALENCE_THRESHOLD, FILTER_SYNTAX_HELP, DistinctValueField, \
    WastewaterGeoBin
from utils.errors import ParsingError

log = logging.getLogger(__name__)

# Tag names used to group the endpoints in the auto-generated docs at /docs.
TAG_SAMPLES = 'Samples'
TAG_VARIANTS = 'Variants'
TAG_MUTATIONS = 'Mutations'
TAG_LINEAGES = 'Lineages'
TAG_WASTEWATER = 'Wastewater'
TAG_PHENOTYPE = 'Phenotype Metrics'
TAG_ANNOTATIONS = 'Annotations'
TAG_DISCOVERY = 'Discovery'

API_DESCRIPTION = (
    'API for querying the Muninn database of viral sequencing samples, their consensus mutations, '
    'intra-host variants, lineage assignments/abundances, phenotype metrics and annotations.\n\n'
    '**Filtering.** Many endpoints take a `filter` query parameter written in a small filter '
    'language. ' + FILTER_SYNTAX_HELP + '\n\n'
    'The *columns* you may reference in a filter differ per endpoint and are listed in each '
    "endpoint's `filter` description. To discover the valid *values* for a column, call "
    '`GET /v1/distinctValues` (grouped under the **Discovery** tag).'
)

TAGS_METADATA = [
    {'name': TAG_SAMPLES, 'description': 'Sample (sequencing run) metadata and sample-level counts/aggregates.'},
    {'name': TAG_VARIANTS, 'description': 'Intra-host (sub-consensus) variants: within-sample minority alleles and their metrics.'},
    {'name': TAG_MUTATIONS, 'description': 'Consensus mutations: changes fixed in a sample\'s consensus sequence, and their counts.'},
    {'name': TAG_LINEAGES, 'description': 'Lineage assignments, abundances, and per-lineage mutation incidence/profiles.'},
    {'name': TAG_PHENOTYPE, 'description': 'Phenotype metrics (e.g. DMS/EVEscape scores) and mutation/variant counts scored by them.'},
    {'name': TAG_ANNOTATIONS, 'description': 'Literature/effect annotations attached to amino-acid changes.'},
    {'name': TAG_WASTEWATER, 'description': 'Wastewater surveillance: abundance-based lineage calls in environmental samples, and their population-weighted averages by location and week.'},
    {'name': TAG_DISCOVERY, 'description': 'Endpoints that enumerate the valid values/keys used to build filter queries (start here before filtering).'},
]

app = FastAPI(
    title='Muninn API',
    description=API_DESCRIPTION,
    version='0.1.0',
    openapi_tags=TAGS_METADATA,
)


def operation_id_from_route(route: APIRoute) -> str:
    """
    FastMCP derives its tool names from FastAPI's operation_id, and truncates them at 56 characters.
    The default id is function name + path + method, which blew that budget on 6 of the routes and cut
    them mid-word ('..._v1_variants_countB'). Deriving the id from the path alone keeps every name
    short, readable and stable if the handler is ever renamed.
    """
    path = route.path.removeprefix('/v1/')
    return re.sub(r'\W+', '_', path).strip('_')


router = APIRouter(prefix='/v1', generate_unique_id_function=operation_id_from_route)


DateBinParam = Annotated[DateBinOpt, Query(
    description='Granularity of the date bins over the collection-window midpoint: month, week, or day. '
                'Applies only where date binning is in effect — on the :count endpoints that means '
                'group_by=collection_date.'
)]
DaysParam = Annotated[int, Query(description='Bin width in days when date_bin=day')]
MaxSpanParam = Annotated[int, Query(
    description='Exclude samples whose collection window (end - start) exceeds this many days. Applies '
                'only where collection-date binning is in effect.'
)]
MinAltFreqParam = Annotated[float | None, Query(
    ge=0, le=1,
    description='Optional lower bound on the intra-host alternate-allele frequency. Frequency is stored '
                'as a 0.05-wide bin, so this selects observations whose bin overlaps the requested '
                'window: 0.9 and 0.92 both return the [0.9,0.95) and [0.95,1] bins, because the database '
                'cannot resolve within a bin. Ingestion discards calls below 0.2.'
)]
MaxAltFreqParam = Annotated[float | None, Query(
    ge=0, le=1,
    description='Optional upper bound on the intra-host alternate-allele frequency, with the same '
                'bin-overlap semantics as min_alt_freq.'
)]


def filter_query(columns_help: str, *, required: bool = True) -> Query:
    """
    Build the OpenAPI `Query` for a `filter` parameter. `columns_help` describes the columns that
    are filterable *for this endpoint*; the shared filter-language grammar (FILTER_SYNTAX_HELP) is
    appended automatically so every filter parameter documents the syntax inline. This keeps the
    grammar in one place and ensures it survives even when the spec is split into per-endpoint tools.
    """
    return Query(
        ... if required else None,
        alias='filter',
        description=f'{columns_help}\n\n{FILTER_SYNTAX_HELP}',
    )

app.add_middleware(
    CORSMiddleware,
    allow_origins=['*'],
    allow_credentials=False,
    allow_methods=['*'],
    allow_headers=['*']
)


_USER_QUERY_SQLSTATES = frozenset({
    '42703',  # undefined_column
    '42702',  # ambiguous_column (e.g. a bare `id` where two joined tables both have one)
    '42883',  # undefined_function / no operator matches the given types
    '42804',  # datatype_mismatch
    '22P02',  # invalid_text_representation (e.g. a non-numeric value for a numeric column)
    '22007',  # invalid_datetime_format
    '22008',  # datetime_field_overflow
    '42601',  # syntax_error: reachable from a filter that parses but emits invalid SQL
})

@app.exception_handler(DBAPIError)
async def handle_db_query_error(request: Request, exc: DBAPIError):
    sqlstate = getattr(getattr(exc, 'orig', None), 'sqlstate', None)
    if sqlstate in _USER_QUERY_SQLSTATES:
        log.error(exc)
        return JSONResponse(
            status_code=400,
            content={
                'detail': 'Invalid filter/group_by: it references a column, value, type, or operator that '
                          'is not valid for this endpoint. See the endpoint filter description for the '
                          'queryable columns.'
            }
        )
    raise exc

@app.exception_handler(ParsingError)
async def handle_parsing_error(request: Request, exc: ParsingError):
    return JSONResponse(status_code=400, content={'detail': exc.message})


def register_log_filter() -> None:
    """
    Avoid cluttering logs with requests to health endpoints
    """

    class EndpointFilter(logging.Filter):
        def filter(self, record: logging.LogRecord) -> bool:
            return (
                record.args
                and len(record.args) >= 3
                and record.args[2] != "/v1/health"
            )

    logging.getLogger("uvicorn.access").addFilter(EndpointFilter())
register_log_filter()


@router.get('/health')
async def health():
    return JSONResponse(status_code=200, content={'alive?': 'you betcha!'})

#############
# DISCOVERY #
#############

@router.get(
    '/distinctValues',
    response_model=List[str],
    tags=[TAG_DISCOVERY],
    summary='List or fuzzy-search the distinct values present for a filterable column (to help build filter queries)'
)
async def get_distinct_values(
    field: DistinctValueField = Query(
        ...,
        description='The column to enumerate distinct, non-null values for. These are the high-value, '
                    'bounded-cardinality string columns most useful when constructing a `filter`: sample '
                    'metadata (host, organism, serotype, platform, instrument, assay_type, library_*, '
                    'isolation_source, center_name, bio_project), geography (country_name, admin1_name, '
                    'admin2_name, admin3_name), genomic change dimensions (region, gff_feature) and lineage '
                    'nomenclature (lineage_system_name). Values are returned sorted ascending, unless '
                    '`search` is given, which orders them by relevance instead.'
    ),
    search: str | None = Query(
        None,
        max_length=100,
        description='Optional fuzzy search over the values, returning the plausible matches best-first '
                    'instead of the whole sorted list. Case-insensitive and typo-tolerant, so '
                    'search=califrnia finds California. It matches spelling, not meaning: searching host '
                    'for "cattle" finds nothing, because the value is "Bos taurus".'
    ),
    where: str | None = filter_query(
        'Optional: narrows which rows contribute values, e.g. field=admin1_name with '
        'filter=country_name = USA lists only US states. Over all columns of the `samples` table plus the '
        'joined `geo_locations` columns (raw names, e.g. country_name, admin1_name). Only supported for the '
        'sample-metadata and geography fields; region, gff_feature and lineage_system_name live outside '
        'that join and return 400 if a filter is given.',
        required=False,
    ),
):
    try:
        return await DB.queries.helpers.get_distinct_values(field, where, search)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))

###########
# SAMPLES #
###########

@router.get('/sample/{sample_id}', response_model=SampleInfo, tags=[TAG_SAMPLES], summary='Get sample metadata by sample ID')
async def get_sample_by_id(sample_id: int = Path(..., description='The ID of the sample to retrieve')):
    sample = await DB.queries.samples.get_sample_by_id(sample_id)
    if sample is None:
        raise HTTPException(status_code=404)
    return sample

@router.get('/samples', response_model=List[SampleInfo], tags=[TAG_SAMPLES], summary='Get samples matching a query')
async def get_samples_query(where: str = filter_query('Over all columns of the `samples` table, plus the joined `geo_locations` columns (use their raw names, e.g. admin1_name, country_name, not the geo_* response names).')):
    return await DB.queries.samples.get_samples(where)

@router.get('/samples:collectionReleaseLag', response_model=List[SampleCollectionReleaseLagInfo], tags=[TAG_SAMPLES], summary='Get collection-to-release lag quartiles, binned by collection-midpoint date')
async def get_sample_collection_release_lag(
    date_bin: DateBinParam = DateBinOpt.month,
    days: DaysParam = DEFAULT_DAYS,
    max_span_days: MaxSpanParam = DEFAULT_MAX_SPAN_DAYS,
):
    return await DB.queries.samples.get_sample_collection_release_lag(max_span_days, date_bin, days)

@router.get('/samples:byMutation', response_model=List[SampleInfo], tags=[TAG_SAMPLES], summary='Get samples carrying a consensus mutation matching a query')
async def get_samples_by_mutation(
    change_bin: NtOrAa = Query(NtOrAa.aa, description='Whether the query filters on nucleotide (nt) allele columns or amino-acid (aa) columns'),
    where: str = filter_query('Over consensus-mutation columns. change_bin=nt: all columns of the `alleles` table (region, position_nt, ref_nt, alt_nt); change_bin=aa: all columns of the `amino_acids` table (position_aa, ref_aa, alt_aa, gff_feature, ref_codon, alt_codon).'),
):
    return await DB.queries.samples.get_samples_by_mutation(change_bin, where)

@router.get('/samples:byVariant', response_model=List[SampleInfo], tags=[TAG_SAMPLES], summary='Get samples carrying an intra-host variant matching a query')
async def get_samples_by_variant(
    change_bin: NtOrAa = Query(NtOrAa.aa, description='Whether the query filters on nucleotide (nt) allele columns or amino-acid (aa) columns'),
    where: str = filter_query('Over the change catalog. change_bin=nt: all columns of the `alleles` table (region, position_nt, ref_nt, alt_nt); change_bin=aa: all columns of the `amino_acids` table (position_aa, ref_aa, alt_aa, gff_feature, ref_codon, alt_codon).'),
    min_alt_freq: MinAltFreqParam = None,
    max_alt_freq: MaxAltFreqParam = None,
):
    return await DB.queries.samples.get_samples_by_variant(change_bin, where, min_alt_freq, max_alt_freq)

@router.get(
    '/samples:count',
    response_model=Dict[str, int] | Dict[str, Dict[str, Dict[str, int]]] | List[LineageCountInfo],
    tags=[TAG_SAMPLES],
    summary='Count samples grouped by field, date, and/or lineage'
)
async def get_sample_counts(
    group_by: Annotated[str, Query(pattern=COMMA_SEP_WORDLIKE_PATTERN.pattern, description='Column to group counts by: a date field (collection_date, release_date, creation_date), "lineage", or any sample column. Optionally "lineage,<date_field>" to also bin by date.')],
    date_bin: DateBinParam = DateBinOpt.month,
    days: DaysParam = DEFAULT_DAYS,
    where: str | None = filter_query('Optional: over all columns of the `samples` table, plus the joined `geo_locations` columns (raw names, e.g. admin1_name, country_name, not the geo_* response names).', required=False),
    max_span_days: MaxSpanParam = DEFAULT_MAX_SPAN_DAYS,
):
    try:
        return await DB.queries.samples.get_sample_counts(group_by, date_bin, days, where, max_span_days)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except NotImplementedError as e:
        raise HTTPException(status_code=501, detail=str(e))

############
# VARIANTS #
############

@router.get(
    '/variants',
    response_model=List[VariantNucleotideInfo] | List[VariantAminoAcidInfo],
    tags=[TAG_VARIANTS],
    summary='Get intra-host variants matching a query'
)
async def get_variants_query(
    change_bin: NtOrAa = Query(NtOrAa.nt, description='Whether the query filters on (and returns) nucleotide (nt) allele variants or amino-acid (aa) variants'),
    where: str = filter_query('Over the change catalog. change_bin=nt: all columns of the `alleles` table (region, position_nt, ref_nt, alt_nt); change_bin=aa: all columns of the `amino_acids` table (position_aa, ref_aa, alt_aa, gff_feature, ref_codon, alt_codon). Read depths and the exact alt_freq are no longer stored, so they cannot be filtered on; use min_alt_freq/max_alt_freq for frequency, since the bin column is a range type the filter language cannot express.'),
    min_alt_freq: MinAltFreqParam = None,
    max_alt_freq: MaxAltFreqParam = None,
):
    return await DB.queries.variants.get_variants(change_bin, where, min_alt_freq, max_alt_freq)

@router.get(
    '/variants:bySample',
    response_model=List[VariantNucleotideInfo] | List[VariantAminoAcidInfo],
    tags=[TAG_VARIANTS],
    summary='Get intra-host variants for all samples matching a sample filter'
)
async def get_variants_by_sample(
    change_bin: NtOrAa = Query(NtOrAa.nt, description='Whether to return nucleotide (nt) allele variants or amino-acid (aa) variants'),
    where: str = filter_query('Selects which samples to return variants for: over all columns of the `samples` table, plus the joined `geo_locations` columns (raw names, e.g. admin1_name, country_name, not the geo_* response names).'),
    min_alt_freq: MinAltFreqParam = None,
    max_alt_freq: MaxAltFreqParam = None,
):
    return await DB.queries.variants.get_variants_by_sample(change_bin, where, min_alt_freq, max_alt_freq)

@router.get(
    '/variants:count',
    response_model=Dict[str, Dict[str, int]] | Dict[str, int],
    tags=[TAG_VARIANTS],
    summary='Count intra-host variant observations grouped by a column or by collection date'
)
async def get_variant_counts(
    group_by: Annotated[str, Query(pattern=WORDLIKE_PATTERN.pattern, description='Grouping key: "collection_date" for a date-binned time series of per-change counts, or a column name to group by. The column must belong to the change_bin catalog: nt=alleles columns (region, position_nt, ref_nt, alt_nt); aa=amino_acids columns (gff_feature, ref_aa, position_aa, alt_aa, ref_codon, alt_codon). "alt_freq_range" also works, to count observations per intra-host frequency bin.')],
    change_bin: NtOrAa = Query(NtOrAa.aa, description='Whether counts are over nucleotide (nt) allele variants or amino-acid (aa) variants.'),
    date_bin: DateBinParam = DateBinOpt.month,
    days: DaysParam = DEFAULT_DAYS,
    max_span_days: MaxSpanParam = DEFAULT_MAX_SPAN_DAYS,
    where: str | None = filter_query('Optional. Selects which *samples* are counted: over all columns of the `samples` table, the joined `geo_locations` columns (raw names, e.g. admin1_name, country_name, not the geo_* response names), and the lineage columns (lineage_name, lineage_system_name). filter can only narrow the sample side. Use group_by to slice the change side.', required=False),
):
    """
    Counts are of (sample, change) observations: a change is counted once per sample carrying it at
    any intra-host frequency, not once per frequency bin.
    """
    if group_by == COLLECTION_DATE:
        return await DB.queries.counts.count_variants_by_collection_date(date_bin, change_bin, days, max_span_days, where)
    return await DB.queries.counts.count_variants_by_column(group_by, change_bin, where)

@router.get(
    '/variants:freqByCollectionDate',
    response_model=List[VariantNucleotideFrequencyByCollectionDateInfo] | List[VariantAminoAcidFrequencyByCollectionDateInfo],
    tags=[TAG_VARIANTS],
    summary='Get intra-host alternate-allele frequency quartiles for each change, binned by collection date'
)
async def get_variant_frequency_by_collection_date(
    change_bin: NtOrAa = Query(NtOrAa.aa, description='Whether quartiles are reported per nucleotide (nt) allele variant or per amino-acid (aa) variant.'),
    date_bin: DateBinParam = DateBinOpt.month,
    days: DaysParam = DEFAULT_DAYS,
    max_span_days: MaxSpanParam = DEFAULT_MAX_SPAN_DAYS,
    where: str | None = filter_query('Optional. Selects which *samples* are included: over all columns of the `samples` table, the joined `geo_locations` columns (raw names, e.g. admin1_name, country_name, not the geo_* response names), and the lineage columns (lineage_name, lineage_system_name). Variant/change columns are not filterable here — an intra-host variant is stored per change, not per sample, so the filter can only narrow the sample side.', required=False),
):
    return await DB.queries.variants.get_variant_frequency_by_collection_date(
        date_bin,
        change_bin,
        days,
        max_span_days,
        where,
    )

@router.get(
    '/variants:sampleFrequency',
    response_model=List[VariantFreqInfo],
    tags=[TAG_VARIANTS],
    summary='Get the intra-host frequency of one change in each sample carrying it'
)
async def get_variant_sample_frequency(
    aa: Annotated[str | None, Query(pattern=CHANGE_PATTERN, description='Amino-acid change to report, as gff_feature:ref<pos>alt (e.g. S:E484K). Provide aa or nt, not both.')] = None,
    nt: Annotated[str | None, Query(pattern=CHANGE_PATTERN, description='Nucleotide change to report, as region:ref<pos>alt (e.g. NC_045512.2:C21T). Provide aa or nt, not both.')] = None,
):
    """
    The per-sample counterpart of /v1/mutations:sampleCount, which counts samples rather than listing
    them. Frequency is a 0.05-wide bin, not an exact value: the database stopped recording exact
    alt_freq. Ingestion also discards intra-host calls below frequency 0.2, so a sample that carries
    the change more faintly than that is absent rather than present with a low frequency.
    """
    if aa is not None and nt is not None:
        raise HTTPException(status_code=400, detail='Provide either an amino-acid (aa) or nucleotide (nt) change, not both')
    if aa is not None:
        return await DB.queries.prevalence.get_samples_variant_freq_by_aa_change(aa)
    if nt is not None:
        return await DB.queries.prevalence.get_samples_variant_freq_by_nt_change(nt)
    raise HTTPException(status_code=400, detail='Provide an amino-acid (aa) or nucleotide (nt) change')

@router.get(
    '/variants:countByPhenotypeScore',
    response_model=List[VariantCountPhenoScoreInfo],
    tags=[TAG_VARIANTS],
    summary='Count samples per intra-host amino-acid variant alongside a phenotype metric value'
)
async def get_variant_counts_by_phenotype_score(
    region: str = Query(..., description='GFF feature (gene/product) to restrict amino-acid variants to'),
    metric: str = Query(..., description='Phenotype metric name whose value is reported per amino-acid change'),
    include_refs: bool = Query(False, description='If true, also include changes where reference amino acid equals alternative amino acid; default false excludes them'),
    where: str | None = filter_query('Optional, restricting which samples are counted: over all columns of the `samples` table, plus the joined `geo_locations` columns (raw names, e.g. admin1_name, country_name, not the geo_* response names).', required=False),
):
    return await DB.queries.prevalence.get_pheno_values_and_variant_counts(metric, region, include_refs, where)

#############
# MUTATIONS #
#############

@router.get(
    '/mutations',
    response_model=List[MutationNucleotideInfo] | List[MutationAminoAcidInfo],
    tags=[TAG_MUTATIONS],
    summary='Get consensus mutations matching a query'
)
async def get_mutations_query(
    change_bin: NtOrAa = Query(NtOrAa.nt, description='Whether the query filters on (and returns) nucleotide (nt) allele mutations or amino-acid (aa) mutations'),
    where: str = filter_query('Over consensus-mutation columns. change_bin=nt: all columns of the `alleles` table (region, position_nt, ref_nt, alt_nt); change_bin=aa: all columns of the `amino_acids` table (position_aa, ref_aa, alt_aa, gff_feature, ref_codon, alt_codon).'),
):
    return await DB.queries.mutations.get_mutations(change_bin, where)

@router.get(
    '/mutations:bySample',
    response_model=List[MutationNucleotideInfo] | List[MutationAminoAcidInfo],
    tags=[TAG_MUTATIONS],
    summary='Get consensus mutations for all samples matching a sample filter'
)
async def get_mutations_by_sample(
    change_bin: NtOrAa = Query(NtOrAa.nt, description='Whether to return nucleotide (nt) allele mutations or amino-acid (aa) mutations'),
    where: str = filter_query('Selects which samples to return mutations for: over all columns of the `samples` table, plus the joined `geo_locations` columns (raw names, e.g. admin1_name, country_name, not the geo_* response names).'),
):
    return await DB.queries.mutations.get_mutations_by_sample(change_bin, where)

@router.get(
    '/mutations:sampleCount',
    response_model=List[MutationCountInfo],
    tags=[TAG_MUTATIONS],
    summary='Count samples carrying a specific consensus mutation'
)
async def get_mutation_sample_count(
    aa: Annotated[str | None, Query(pattern=CHANGE_PATTERN, description='Amino-acid change to count, as gff_feature:ref<pos>alt (e.g. S:E484K). Provide aa or nt, not both.')] = None,
    nt: Annotated[str | None, Query(pattern=CHANGE_PATTERN, description='Nucleotide change to count, as region:ref<pos>alt (e.g. NC_045512.2:C21T). Provide aa or nt, not both.')] = None,
):
    if aa is not None and nt is not None:
        raise HTTPException(status_code=400, detail='Provide either an amino-acid (aa) or nucleotide (nt) change, not both')
    if aa is not None:
        return await DB.queries.prevalence.get_mutation_sample_count_by_aa(aa)
    if nt is not None:
        return await DB.queries.prevalence.get_mutation_sample_count_by_nt(nt)
    raise HTTPException(status_code=400, detail='Provide an amino-acid (aa) or nucleotide (nt) change')

@router.get(
    '/mutations:gffFeatures',
    response_model=List[str],
    tags=[TAG_MUTATIONS],
    summary='List the distinct GFF feature (gene/product) names'
)
async def get_mutation_gff_features():
    return await DB.queries.helpers.get_gff_features()

@router.get(
    '/mutations:count',
    response_model=Dict[str, Dict[str, int]] | Dict[str, int],
    tags=[TAG_MUTATIONS],
    summary='Count consensus mutations grouped by a column or by collection date'
)
async def get_mutation_counts(
    group_by: Annotated[str, Query(pattern=WORDLIKE_PATTERN.pattern, description='Grouping key: "collection_date" for a date-binned time series of per-mutation counts, or a column name to group by. The column must belong to the change_bin catalog: nt=alleles columns (region, position_nt, ref_nt, alt_nt); aa=amino_acids columns (gff_feature, ref_aa, position_aa, alt_aa, ref_codon, alt_codon).')],
    change_bin: NtOrAa = Query(NtOrAa.aa, description='Whether counts are over nucleotide (nt) allele mutations or amino-acid (aa) mutations.'),
    date_bin: DateBinParam = DateBinOpt.month,
    days: DaysParam = DEFAULT_DAYS,
    max_span_days: MaxSpanParam = DEFAULT_MAX_SPAN_DAYS,
    where: str | None = filter_query('Optional. With group_by=collection_date: over all `samples` columns, the joined `geo_locations` columns, the change columns (nt: region, ref_nt, position_nt, alt_nt; aa: gff_feature, ref_aa, position_aa, alt_aa, ref_codon, alt_codon) and the lineage columns (lineage_name, lineage_system_name). With any other group_by the filter selects samples only, so it accepts `samples` and `geo_locations` columns — change and lineage columns return 400 there.', required=False),
):
    if group_by == COLLECTION_DATE:
        return await DB.queries.counts.count_mutations_by_collection_date(date_bin, change_bin, days, max_span_days, where)
    return await DB.queries.counts.count_mutations_by_column(group_by, change_bin, where)

@router.get(
    '/mutations:variantLag',
    response_model=Dict[str, List[VariantMutationLagInfo]],
    tags=[TAG_MUTATIONS],
    summary='Get amino-acid changes seen as consensus mutations before intra-host variants, keyed by GFF feature'
)
async def get_mutations_before_variants(
    lineage: str = Query(..., description='Lineage name to restrict samples to (e.g. BA.1)'),
    lineage_system_name: str = Query(..., description='Name of the lineage nomenclature system the lineage belongs to (e.g. a Pango/Nextstrain lineage)')
):
    return await DB.queries.variants_mutations.get_mutations_before_variants(lineage, lineage_system_name)

@router.get(
    '/variants:mutationLag',
    response_model=Dict[str, List[VariantMutationLagInfo]],
    tags=[TAG_VARIANTS],
    summary='Get amino-acid changes seen as intra-host variants before consensus mutations, keyed by GFF feature'
)
async def get_variants_before_mutations(
    lineage: str = Query(..., description='Lineage name to restrict samples to (e.g. BA.1)'),
    lineage_system_name: str = Query(..., description='Name of the lineage nomenclature system the lineage belongs to (e.g. a Pango/Nextstrain lineage)'),
):
    return await DB.queries.variants_mutations.get_variants_before_mutations(lineage, lineage_system_name)

@router.get(
    '/mutations:countByPhenotypeScore',
    response_model=List[VariantCountPhenoScoreInfo],
    tags=[TAG_MUTATIONS],
    summary='Count samples per consensus amino-acid mutation alongside a phenotype metric value'
)
async def get_mutation_counts_by_phenotype_score(
    region: str = Query(..., description='GFF feature (gene/product) to restrict amino-acid mutations to'),
    metric: str = Query(..., description='Phenotype metric name whose value is reported per amino-acid change'),
    include_refs: bool = Query(False, description='If true, also include changes where reference amino acid equals alternative amino acid; default false excludes them'),
    where: str | None = filter_query('Optional, restricting which samples are counted: over all columns of the `samples` table, plus the joined `geo_locations` columns (raw names, e.g. admin1_name, country_name, not the geo_* response names).', required=False),
):
    return await DB.queries.prevalence.get_pheno_values_and_mutation_counts(metric, region, include_refs, where)

@router.get(
    '/mutations:countByCollectionDateAndLineage',
    response_model=List[MutationNucleotideCountByDateAndLineageInfo] | List[MutationAminoAcidCountByDateAndLineageInfo],
    tags=[TAG_MUTATIONS],
    summary='Count samples carrying a specific consensus mutation, binned by collection date and lineage'
)
async def get_mutation_count_by_collection_date_and_lineage(
    change_bin: NtOrAa = Query(..., description='Whether the specified change is a nucleotide (nt) allele mutation or an amino-acid (aa) mutation'),
    position: int = Query(..., description='1-based position of the change: position_nt (nt) or position_aa (aa)'),
    alt: str = Query(..., description='Alternate (mutant) nucleotide (nt) or amino acid (aa) of the change'),
    region: str = Query(..., description='For change_bin=nt: the genomic region/segment (alleles.region). For change_bin=aa: the GFF feature / gene (amino_acids.gff_feature).'),
    date_bin: DateBinParam = DateBinOpt.month,
    days: DaysParam = DEFAULT_DAYS,
    where: str | None = filter_query('Optional, restricting which samples are counted: over all columns of the `samples` table, plus the joined `lineages` columns (e.g. lineage_name). No geo columns are joined here.', required=False),
    max_span_days: MaxSpanParam = DEFAULT_MAX_SPAN_DAYS,
):
    if change_bin == NtOrAa.nt:
        return await DB.queries.mutations.get_nt_mutation_count_by_collection_date(date_bin, position, alt, region, days, max_span_days, where)
    return await DB.queries.mutations.get_aa_mutation_count_by_collection_date(date_bin, position, alt, region, days, max_span_days, where)

############
# LINEAGES #
############

@router.get(
    '/lineages',
    response_model=List[LineageInfo],
    tags=[TAG_LINEAGES],
    summary='List the lineages belonging to a lineage system'
)
async def get_lineages_by_lineage_system(
    lineage_system_name: str = Query(..., description='Name of the lineage nomenclature system to list lineages for, matched against lineage_systems.lineage_system_name (e.g. PANGO)'),
):
    return await DB.queries.lineages.get_all_lineages_by_lineage_system(lineage_system_name)

@router.get(
    '/lineages:abundance',
    response_model=Dict[str, List[LineageAbundanceSummaryInfo]]
                   | List[LineageAbundanceInfo]
                   | List[LineageAbundanceSummaryInfo],
    tags=[TAG_LINEAGES],
    summary='Get lineage abundances (per-sample) or abundance summary stats, optionally binned by date'
)
async def get_lineage_abundance(
    group_by: Annotated[str | None, Query(pattern=WORDLIKE_PATTERN.pattern, description='Optional date field to bin summaries by: only "collection_date" is supported. Omit to aggregate/list over all samples.')] = None,
    date_bin: DateBinParam = DateBinOpt.month,
    days: DaysParam = DEFAULT_DAYS,
    where: str | None = filter_query('Optional: over all `samples` columns, plus joined `lineages`/`lineage_systems` columns (lineage_name, lineage_system_name) and `samples_lineages` (abundance); geo columns join in via their raw names too. Only abundance-based (non-consensus) lineage calls are ever included.', required=False),
    summary: bool = Query(True, description='If true (default) return per-lineage abundance summary stats; if false return per-sample abundance rows (only supported when group_by is omitted)'),
    max_span_days: MaxSpanParam = DEFAULT_MAX_SPAN_DAYS,
):
    if group_by == COLLECTION_DATE:
        if summary:
            return await DB.queries.lineages.get_abundance_summaries_by_collection_date(date_bin, days, where, max_span_days)
        raise HTTPException(status_code=501, detail='Per-sample abundances binned by date are not implemented; use summary=true')
    else:
        if summary:
            return await DB.queries.lineages.get_abundance_summaries(where)
        return await DB.queries.lineages.get_abundances(where)

@router.get(
    '/lineages:countByCollectionDate',
    response_model=Dict[str, List[LineageCountWithPrevalenceInfo]],
    tags=[TAG_LINEAGES],
    summary='Per-lineage sample counts and prevalence over time (collection-date binned)'
)
async def get_lineage_counts_over_time(
    where: str | None = filter_query(
        'Optional: over `samples`, joined `geo_locations` (raw names, e.g. country_name, admin1_name) and '
        '`lineages`/`lineage_systems` (lineage_name, lineage_system_name) columns. Counts consensus lineage '
        'calls only (abundance/wastewater calls excluded).',
        required=False,
    ),
    lineage: str | None = Query(None, description='Optional: restrict to a single lineage (lineages.lineage_name); omit for all lineages'),
    date_bin: DateBinParam = DateBinOpt.month,
    days: DaysParam = DEFAULT_DAYS,
    max_span_days: MaxSpanParam = DEFAULT_MAX_SPAN_DAYS,
    days_before_today: int | None = Query(None, gt=0, description='Optional: only count samples whose collection midpoint is within this many days before today'),
):
    return await DB.queries.lineages.get_lineage_counts_over_time(
        date_bin, days, where, max_span_days, days_before_today, lineage
    )

@router.get(
    '/lineages:mutationIncidence',
    response_model=MutationIncidenceInfo,
    tags=[TAG_LINEAGES],
    summary='Get consensus mutations prevalent within a lineage above a threshold'
)
async def get_mutation_incidence(
    lineage: str = Query(..., description='Lineage name to compute mutation incidence for, matched against lineages.lineage_name (e.g. BA.2)'),
    lineage_system_name: str = Query(..., description='Lineage nomenclature system the lineage belongs to, matched against lineage_systems.lineage_system_name (e.g. PANGO)'),
    change_bin: NtOrAa = Query(..., description='Report nucleotide (nt) or amino-acid (aa) consensus mutations'),
    prevalence_threshold: float = Query(DEFAULT_PREVALENCE_THRESHOLD, description=f'Minimum fraction of the lineage samples carrying a mutation for it to be returned (minimum allowed: {MIN_PREVALENCE_THRESHOLD})'),
    match_reference: bool = Query(False, description='If false (default) exclude changes where ref == alt; if true include them'),
    where: str | None = filter_query('Optional: over all `samples` columns plus the joined `lineages`/`lineage_systems`/`samples_lineages` columns (e.g. lineage_name, lineage_system_name). Note: geo_locations columns are NOT joined here and cannot be filtered on.', required=False),
):
    if prevalence_threshold < MIN_PREVALENCE_THRESHOLD:
        raise HTTPException(status_code=400, detail=f'minimum allowed prevalence threshold is {MIN_PREVALENCE_THRESHOLD}')

    return await DB.queries.lineages.get_mutation_incidence(
        lineage,
        lineage_system_name,
        change_bin,
        prevalence_threshold,
        match_reference,
        where
    )

@router.get(
    '/lineages:mutationProfile',
    response_model=List[MutationProfileWithPrevalenceInfo],
    tags=[TAG_LINEAGES],
    summary="Get a lineage's single-nucleotide mutation spectrum (counts and per-region prevalence per ref→alt substitution class and region)"
)
async def get_mutation_profile(
    lineage: str = Query(..., description='Lineage name to compute the mutation spectrum for, matched against lineages.lineage_name (e.g. BA.2)'),
    lineage_system_name: str = Query(..., description='Lineage nomenclature system the lineage belongs to, matched against lineage_systems.lineage_system_name (e.g. PANGO)'),
    where: str | None = filter_query('Optional: over all `samples` columns plus the joined `lineages`/`lineage_systems`/`samples_lineages` columns (e.g. lineage_name, lineage_system_name). Note: geo_locations and alleles columns are NOT joined here and cannot be filtered on.', required=False),
):
    return await DB.queries.lineages.get_mutation_profile(lineage, lineage_system_name, where)


####################
# PHENOTYPE METRICS #
####################

@router.get(
    '/phenotypeMetrics',
    response_model=List[PhenotypeMetricInfo],
    tags=[TAG_PHENOTYPE],
    summary='List all available phenotype metrics'
)
async def get_all_phenotype_metrics():
    return await DB.queries.phenotype_metrics.get_all_pheno_metrics()

@router.get(
    '/phenotypeMetricValues:countMutationsByCollectionDate',
    response_model=List[PhenotypeMetricDateCountInfo],
    tags=[TAG_PHENOTYPE],
    summary='Count phenotype-scored consensus mutations at/above a threshold, binned by collection date'
)
async def get_phenotype_metric_count_mutations_by_collection_date(
    phenotype_metric_name: str = Query(..., description='Phenotype metric to score amino-acid changes by, matched against phenotype_metrics.phenotype_metric_name (e.g. delta_bind)'),
    phenotype_metric_value_threshold: float = Query(..., description='Threshold on the metric value; n_gte counts scored amino-acid changes whose value is >= this'),
    date_bin: DateBinParam = DateBinOpt.month,
    days: DaysParam = DEFAULT_DAYS,
    where: str | None = filter_query('Optional: over all `samples` columns plus the joined `geo_locations` columns (raw names, e.g. admin1_name/country_name) and `lineages`/`lineage_systems` columns (lineage_name, lineage_system_name). alleles/amino_acids columns are NOT joined and cannot be filtered on.', required=False),
    max_span_days: MaxSpanParam = DEFAULT_MAX_SPAN_DAYS,
):
    return await DB.queries.phenotype_metrics.count_mutations_gte_pheno_value_by_collection_date(
        date_bin,
        phenotype_metric_name,
        phenotype_metric_value_threshold,
        days,
        max_span_days,
        where,
    )

@router.get(
    '/phenotypeMetricValues:countVariantsByCollectionDate',
    response_model=List[PhenotypeMetricDateCountInfo],
    tags=[TAG_PHENOTYPE],
    summary='Count phenotype-scored intra-host variants at/above a threshold, binned by collection date'
)
async def get_phenotype_metric_count_variants_by_collection_date(
    phenotype_metric_name: str = Query(..., description='Phenotype metric to score amino-acid changes by, matched against phenotype_metrics.phenotype_metric_name (e.g. delta_bind)'),
    phenotype_metric_value_threshold: float = Query(..., description='Threshold on the metric value; n_gte counts scored amino-acid changes whose value is >= this'),
    date_bin: DateBinParam = DateBinOpt.month,
    days: DaysParam = DEFAULT_DAYS,
    where: str | None = filter_query('Optional, selecting which samples are binned: over all `samples` columns, the joined `geo_locations` columns (raw names, e.g. admin1_name/country_name), and `lineages`/`lineage_systems` columns (lineage_name, lineage_system_name). Amino-acid columns are not filterable — the metric already selects which changes are counted.', required=False),
    max_span_days: MaxSpanParam = DEFAULT_MAX_SPAN_DAYS,
    min_alt_freq: MinAltFreqParam = None,
    max_alt_freq: MaxAltFreqParam = None,
):
    return await DB.queries.phenotype_metrics.count_variants_gte_pheno_value_by_collection_date(
        date_bin,
        phenotype_metric_name,
        phenotype_metric_value_threshold,
        days,
        max_span_days,
        where,
        min_alt_freq,
        max_alt_freq,
    )

@router.get(
    '/phenotypeMetricValues:forMutationsAggregateBySampleAndCollectionDate',
    operation_id='phenotypeMetricValues_mutationAggregatesByDate',
    response_model=List[PhenotypeMetricAggregateByDateInfo],
    tags=[TAG_PHENOTYPE],
    summary='Per-collection-date quartiles of per-sample consensus-mutation phenotype load (summed value and distinct-aa count)'
)
async def get_phenotype_metric_values_for_mutations_by_sample_and_collection_date(
    phenotype_metric_name: str = Query(..., description='Phenotype metric to score amino-acid changes by, matched against phenotype_metrics.phenotype_metric_name (e.g. delta_bind)'),
    date_bin: DateBinParam = DateBinOpt.month,
    days: DaysParam = DEFAULT_DAYS,
    where: str | None = filter_query('Optional: over all `samples` columns plus the joined `geo_locations` columns (raw names, e.g. admin1_name/country_name) and `lineages`/`lineage_systems` columns (lineage_name, lineage_system_name). alleles/amino_acids columns are NOT joined and cannot be filtered on.', required=False),
    max_span_days: MaxSpanParam = DEFAULT_MAX_SPAN_DAYS,
):
    return await DB.queries.phenotype_metrics.get_pheno_value_for_mutations_by_sample_and_collection_date(
        date_bin,
        phenotype_metric_name,
        days,
        max_span_days,
        where,
    )

@router.get(
    '/phenotypeMetricValues:forVariantsAggregateBySampleAndCollectionDate',
    operation_id='phenotypeMetricValues_variantAggregatesByDate',
    response_model=List[PhenotypeMetricAggregateByDateInfo],
    tags=[TAG_PHENOTYPE],
    summary='Per-collection-date quartiles of per-sample intra-host-variant phenotype load (summed value and distinct-aa count)'
)
async def get_phenotype_metric_values_for_variants_by_sample_and_collection_date(
    phenotype_metric_name: str = Query(..., description='Phenotype metric to score amino-acid changes by, matched against phenotype_metrics.phenotype_metric_name (e.g. delta_bind)'),
    date_bin: DateBinParam = DateBinOpt.month,
    days: DaysParam = DEFAULT_DAYS,
    where: str | None = filter_query('Optional: over all `samples` columns plus the joined `geo_locations` columns (raw names, e.g. admin1_name/country_name) and `lineages`/`lineage_systems` columns (lineage_name, lineage_system_name). alleles/amino_acids columns are NOT joined and cannot be filtered on.', required=False),
    max_span_days: MaxSpanParam = DEFAULT_MAX_SPAN_DAYS,
):
    return await DB.queries.phenotype_metrics.get_pheno_value_for_variants_by_sample_and_collection_date(
        date_bin,
        phenotype_metric_name,
        days,
        max_span_days,
        where,
    )

@router.get(
    '/phenotypeMetricValues:byMutationsQuantile',
    response_model=Dict[str, float | None],
    tags=[TAG_PHENOTYPE],
    summary='Get the value of a phenotype metric at a given quantile across the substitutions seen in consensus'
)
async def get_phenotype_metric_value_by_mutation_quantile(
    phenotype_metric_name: str = Query(..., description='Phenotype metric whose value distribution is quantiled, matched against phenotype_metrics.phenotype_metric_name (e.g. delta_bind). phenotype_metric_value is null if the metric name is unknown, or if no substitution it scores has been seen in consensus.'),
    quantile: float = Query(..., ge=0.0, le=1.0, description='Quantile in [0,1] (e.g. 0.5 for the median), evaluated with percentile_disc over the non-zero scored-substitution values'),
):
    return await DB.queries.phenotype_metrics.get_phenotype_metric_value_by_mutation_quantile(
        phenotype_metric_name,
        quantile,
    )

@router.get(
    '/phenotypeMetricValues:byVariantsQuantile',
    response_model=Dict[str, float | None],
    tags=[TAG_PHENOTYPE],
    summary='Get the value of a phenotype metric at a given quantile across the substitutions seen intra-host'
)
async def get_phenotype_metric_value_by_variant_quantile(
    phenotype_metric_name: str = Query(..., description='Phenotype metric whose value distribution is quantiled, matched against phenotype_metrics.phenotype_metric_name (e.g. delta_bind). phenotype_metric_value is null if the metric name is unknown, or if no substitution it scores has ever been seen intra-host.'),
    quantile: float = Query(..., ge=0.0, le=1.0, description='Quantile in [0,1] (e.g. 0.5 for the median), evaluated with percentile_disc over the non-zero scored-substitution values'),
):
    return await DB.queries.phenotype_metrics.get_phenotype_metric_value_by_variant_quantile(
        phenotype_metric_name,
        quantile,
    )

@router.get(
    '/phenotypeMetricValues:minAndMaxValues',
    response_model=List[float | None],
    tags=[TAG_PHENOTYPE],
    summary='Get the [min, max] values of a phenotype metric'
)
async def get_phenotype_metric_value_min_and_max(
    phenotype_metric_name: str = Query(..., description='Phenotype metric whose value range is returned, matched against phenotype_metrics.phenotype_metric_name (e.g. delta_bind). Returns [min, max]; [null, null] if the metric name is unknown.'),
):
    return await DB.queries.phenotype_metrics.get_min_max_pheno_metric_value(phenotype_metric_name)


###############
# ANNOTATIONS #
###############

@router.get(
    '/annotations:byMutationsAndCollectionDate',
    response_model=List[AnnotationProportionByDateInfo],
    tags=[TAG_ANNOTATIONS],
    summary='Proportion of annotated consensus-mutation amino acids carrying an annotation effect, binned by collection date'
)
async def get_annotations_by_mutations_and_collection_date(
    effect_detail: str = Query(..., description='Annotation effect to match, compared against effects.detail'),
    date_bin: DateBinParam = DateBinOpt.month,
    days: DaysParam = DEFAULT_DAYS,
    max_span_days: MaxSpanParam = DEFAULT_MAX_SPAN_DAYS,
    where: str | None = filter_query('Optional: over all `samples` columns plus the joined `geo_locations` columns (raw names, e.g. admin1_name/country_name) and `lineages`/`lineage_systems` columns (lineage_name, lineage_system_name). alleles/amino_acids columns are NOT joined and cannot be filtered on.', required=False),
):
    return await DB.queries.annotations.get_annotations_by_mutations_and_collection_date(
        effect_detail,
        date_bin,
        days,
        max_span_days,
        where,
    )

@router.get(
    '/annotations:byVariantsAndCollectionDate',
    response_model=List[AnnotationProportionByDateInfo],
    tags=[TAG_ANNOTATIONS],
    summary='Proportion of annotated intra-host-variant amino acids carrying an annotation effect, binned by collection date'
)
async def get_annotations_by_variants_and_collection_date(
    effect_detail: str = Query(..., description='Annotation effect to match, compared against effects.detail'),
    date_bin: DateBinParam = DateBinOpt.month,
    days: DaysParam = DEFAULT_DAYS,
    max_span_days: MaxSpanParam = DEFAULT_MAX_SPAN_DAYS,
    where: str | None = filter_query(
        'Optional: over all `samples` columns plus the joined `geo_locations` columns (raw names, e.g. '
        'admin1_name/country_name) and `lineages`/`lineage_systems` columns (lineage_name, '
        'lineage_system_name). alleles/amino_acids columns are NOT joined and cannot be filtered on.',
        required=False,
    ),
):
    return await DB.queries.annotations.get_annotations_by_variants_and_collection_date(
        effect_detail,
        date_bin,
        days,
        max_span_days,
        where
    )

@router.get(
    '/annotations:effects',
    response_model=List[str],
    tags=[TAG_ANNOTATIONS],
    summary='List all distinct annotation effect types (effects.detail)'
)
async def get_annotation_effects() -> List[str]:
    return await DB.queries.annotations.get_all_annotation_effects()

@router.get(
    '/annotations:byVariantsAndAminoAcidPosition',
    response_model=Dict[str, List[AnnotatedPositionCountInfo]],
    tags=[TAG_ANNOTATIONS],
    summary='Per-position sample counts of annotated intra-host-variant amino acids for an annotation effect'
)
async def get_annotations_by_variants_and_amino_acid_position(
    effect_detail: str = Query(..., description='Annotation effect to match, compared against effects.detail'),
    where: str | None = filter_query(
        'Optional: over all `samples` columns plus the joined `geo_locations` columns (raw names, e.g. '
        'admin1_name/country_name) and `lineages`/`lineage_systems` columns (lineage_name, '
        'lineage_system_name). alleles/amino_acids columns are NOT joined and cannot be filtered on.',
        required=False,
    ),
):
    return await DB.queries.annotations.get_annotations_by_variants_and_amino_acid_position(effect_detail, where)

@router.get(
    '/annotations:byMutationsAndAminoAcidPosition',
    response_model=Dict[str, List[AnnotatedPositionCountInfo]],
    tags=[TAG_ANNOTATIONS],
    summary='Per-position sample counts of annotated consensus-mutation amino acids for an annotation effect'
)
async def get_annotations_by_mutations_and_amino_acid_position(
    effect_detail: str = Query(..., description='Annotation effect to match, compared against effects.detail'),
    where: str | None = filter_query('Optional: over all `samples` columns plus the joined `geo_locations` columns (raw names, e.g. admin1_name/country_name) and `lineages`/`lineage_systems` columns (lineage_name, lineage_system_name). alleles/amino_acids columns are NOT joined and cannot be filtered on.', required=False),
):
    return await DB.queries.annotations.get_annotations_by_mutations_and_amino_acid_position(
        effect_detail,
        where,
    )


##############
# WASTEWATER #
##############

@router.get(
    '/wastewater/lineages:abundancesBySample',
    response_model=List[LineageAbundanceWithSampleInfo],
    tags=[TAG_WASTEWATER],
    summary='Get per-sample wastewater lineage abundances, with the sampling-site metadata'
)
async def get_wastewater_lineage_abundances_by_sample(
    where: str | None = filter_query(
        'Optional: over all `samples` columns (including the wastewater ones: ww_viral_load, '
        'ww_catchment_population, ww_site_id, ww_collected_by, census_region), the joined '
        '`geo_locations` columns (raw names, e.g. admin1_name, country_name) and the `lineages` '
        'columns (lineage_name).',
        required=False,
    ),
):
    """
    One row per (sample, lineage) call. Unlike /v1/lineages:abundance this is not restricted to
    abundance-based calls, so consensus calls appear here with a null abundance.
    """
    return await DB.queries.wastewater.get_lineage_abundances_by_sample(where)

@router.get(
    '/wastewater/lineages:averageAbundancesByLocation',
    response_model=List[AverageLineageAbundanceInfo],
    tags=[TAG_WASTEWATER],
    summary='Get population-weighted average lineage abundances by location and week'
)
async def get_wastewater_average_lineage_abundances_by_location(
    geo_bin: WastewaterGeoBin = Query(
        WastewaterGeoBin.admin1_name,
        description='Geographic grouping: admin1_name (state/province) or census_region. With '
                    'census_region the response\'s geo_admin1_name is always null, and a filter '
                    'mentioning admin1_name is rejected with a 400 rather than silently ignored.'
    ),
    lineage: str | None = Query(
        None,
        description="Optional lineage name to report. A trailing '*' (e.g. B.1.1.7*) aggregates the "
                    "lineage together with all of its descendants into one series, and the response's "
                    "lineage_name keeps the '*'. Omit to get every lineage separately."
    ),
    where: str | None = filter_query(
        'Optional: over all `samples` columns (including the wastewater ones), the joined '
        '`geo_locations` columns (raw names, e.g. admin1_name, country_name) and the `lineages` '
        'columns (lineage_name).',
        required=False,
    ),
    max_span_days: MaxSpanParam = DEFAULT_MAX_SPAN_DAYS,
):
    """
    Abundances are weighted by each site's catchment population before being averaged, so a large
    catchment counts for more than a small one. Bins are ISO weeks over the collection-window
    midpoint. mean_lineage_prevalence is this lineage's share of the bin's total weighted abundance.
    """
    try:
        return await DB.queries.wastewater.get_averaged_lineage_abundances_by_location(
            where,
            geo_bin.value,
            max_span_days,
            lineage,
        )
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))

@router.get(
    '/wastewater/lineages:count',
    response_model=Dict[str, int],
    tags=[TAG_WASTEWATER],
    summary='Count samples per lineage, over the samples that carry abundance data'
)
async def get_wastewater_lineage_counts(
    where: str | None = filter_query(
        'Optional: over all `samples` columns (including the wastewater ones), the joined '
        '`geo_locations`, `samples_lineages` and `lineages` columns.',
        required=False,
    ),
):
    return await DB.queries.wastewater.count_lineages_by_sample_data(where)

@router.get(
    '/wastewater/samples:count',
    response_model=Dict[str, int],
    tags=[TAG_WASTEWATER],
    summary='Count the samples that carry abundance data, grouped by a column'
)
async def get_wastewater_sample_counts(
    group_by: Annotated[str, Query(
        pattern=COMMA_SEP_WORDLIKE_PATTERN.pattern,
        description='Column to group counts by: any `samples` or joined `geo_locations` column, e.g. '
                    'admin1_name or ww_site_id.'
    )],
    where: str | None = filter_query(
        'Optional: over all `samples` columns (including the wastewater ones), the joined '
        '`geo_locations` and `samples_lineages` columns.',
        required=False,
    ),
):
    """
    Only samples with a non-null abundance are counted, i.e. those with an abundance-based lineage
    call. Use /v1/samples:count to count samples without that restriction.
    """
    return await DB.queries.wastewater.count_samples_with_lineage_data(group_by, where)

@router.get(
    '/wastewater/samples:latest',
    response_model=List[SampleInfo],
    tags=[TAG_WASTEWATER],
    summary='Get the most recently collected sample(s), to show how current the data is'
)
async def get_wastewater_latest_sample(
    where: str | None = filter_query(
        'Optional: over all columns of the `samples` table, plus the joined `geo_locations` columns '
        '(raw names, e.g. admin1_name, country_name, not the geo_* response names). The filter '
        'narrows the field the maximum is taken over, so filter=admin1_name = California returns '
        "California's latest sample rather than nothing.",
        required=False,
    ),
):
    """
    Returns every sample tied for the latest collection_start_date, so this is a list rather than a
    single object. Samples with no collection date are excluded.
    """
    return await DB.queries.wastewater.get_latest_sample(where)


app.include_router(router)


#######
# MCP #
#######

mcp = FastMCP.from_fastapi(app=app, name='Muninn MCP')

mcp_app = mcp.http_app(path='/')

app.router.lifespan_context = mcp_app.lifespan
app.mount('/mcp', mcp_app)
