import difflib
from typing import List, Type

from sqlalchemy import text

from api.models import RegionAndGffFeatureInfo
from DB.engine import get_async_session
from DB.models import IntraHostVariant, Mutation
from parser.parser import parser
from utils.constants import (
    MIN_FUZZY_MATCH_SCORE,
    ColumnNames,
    DistinctValueField,
    TableNames,
)


async def get_gff_features() -> List[str]:
    async with get_async_session() as session:
        res = await session.execute(
            text(
                f"""
                select distinct {ColumnNames.gff_feature} from {TableNames.amino_acids}
                """
            )
        )
    return [row[0] for row in res.all()]


# Maps each enumerable field to the (table, column) it lives in. Both identifiers come from our own
# constants (never from user input), so interpolating them into the query below is injection-safe;
# the field itself is validated by FastAPI against the DistinctValueField enum.
_DISTINCT_VALUE_SOURCES: dict[DistinctValueField, tuple[str, str]] = {
    DistinctValueField.host: (TableNames.samples, ColumnNames.host),
    DistinctValueField.organism: (TableNames.samples, ColumnNames.organism),
    DistinctValueField.serotype: (TableNames.samples, ColumnNames.serotype),
    DistinctValueField.platform: (TableNames.samples, ColumnNames.platform),
    DistinctValueField.instrument: (TableNames.samples, ColumnNames.instrument),
    DistinctValueField.assay_type: (TableNames.samples, ColumnNames.assay_type),
    DistinctValueField.library_selection: (
        TableNames.samples,
        ColumnNames.library_selection,
    ),
    DistinctValueField.library_source: (TableNames.samples, ColumnNames.library_source),
    DistinctValueField.library_layout: (TableNames.samples, ColumnNames.library_layout),
    DistinctValueField.isolation_source: (
        TableNames.samples,
        ColumnNames.isolation_source,
    ),
    DistinctValueField.center_name: (TableNames.samples, ColumnNames.center_name),
    DistinctValueField.bio_project: (TableNames.samples, ColumnNames.bio_project),
    DistinctValueField.country_name: (
        TableNames.geo_locations,
        ColumnNames.country_name,
    ),
    DistinctValueField.admin1_name: (TableNames.geo_locations, ColumnNames.admin1_name),
    DistinctValueField.admin2_name: (TableNames.geo_locations, ColumnNames.admin2_name),
    DistinctValueField.admin3_name: (TableNames.geo_locations, ColumnNames.admin3_name),
    DistinctValueField.region: (TableNames.alleles, ColumnNames.region),
    DistinctValueField.gff_feature: (TableNames.amino_acids, ColumnNames.gff_feature),
    DistinctValueField.lineage_system_name: (
        TableNames.lineage_systems,
        ColumnNames.lineage_system_name,
    ),
}


def rank_by_fuzzy_match(search: str, values: List[str]) -> List[str]:
    needle = search.strip().lower()
    scored = []
    for value in values:
        haystack = value.lower()
        position = haystack.find(needle)
        score = (
            1.0
            if position >= 0
            else difflib.SequenceMatcher(None, needle, haystack).ratio()
        )
        if score >= MIN_FUZZY_MATCH_SCORE:
            scored.append((-score, position, len(value), value))
    scored.sort()
    return [value for _, _, _, value in scored]


async def get_distinct_values(
    field: DistinctValueField,
    filter: str | None = None,
    search: str | None = None,
) -> List[str]:
    """
    Distinct non-null values of `field`, alphabetical.

    `filter` narrows which rows contribute values (e.g. only the admin1_names of samples from USA).
    `search` instead ranks the values by how well they match a search string, best first, dropping
    the ones that don't plausibly match.
    """
    table, column = _DISTINCT_VALUE_SOURCES[field]

    if filter is None:
        # Read the column's own table directly: for the geo fields that means scanning a few
        # thousand rows rather than every sample.
        query = f"select distinct {column} from {table} where {column} is not null order by {column}"
    else:
        # A filter is evaluated over samples joined to geo_locations, so it can only enumerate a
        # column living in one of those two. region, gff_feature and lineage_system_name sit outside
        # that join and are rejected rather than having their filter silently ignored.
        if table not in (TableNames.samples, TableNames.geo_locations):
            raise ValueError(
                f"field={field} does not support a filter: {table} is not joined to samples here."
            )
        query = f"""
        select distinct {table}.{column}
        from {TableNames.samples}
        left join {TableNames.geo_locations}
            on {TableNames.geo_locations}.id = {TableNames.samples}.{ColumnNames.geo_location_id}
        where {table}.{column} is not null and ({parser.parse(filter)})
        order by 1
        """

    async with get_async_session() as session:
        res = await session.execute(text(query))
    values = [row[0] for row in res.all()]

    # A blank search is the caller saying "no search", not a search that matches everything.
    if search is None or search.strip() == "":
        return values
    return rank_by_fuzzy_match(search, values)


async def get_region_and_gff_features(
    intermediate: Type[Mutation] | Type[IntraHostVariant],
) -> List["RegionAndGffFeatureInfo"]:
    translations_table, translations_join_id = (
        get_appropriate_translations_table_and_id(intermediate)
    )
    async with get_async_session() as session:
        res = await session.execute(
            text(
                f"""
                select distinct gff_feature, region
                from {TableNames.amino_acids} aa
                inner join {translations_table} t on t.{ColumnNames.amino_acid_id} = aa.id
                inner join {intermediate.__tablename__} inter on inter.id = t.{translations_join_id}
                inner join {TableNames.alleles} a on a.id = inter.{ColumnNames.allele_id}
                """
            )
        )
    return [RegionAndGffFeatureInfo(**row) for row in res.mappings().all()]


# eventually-do: this doesn't really belong here because it's not a database query, but for now this is fine.
def get_appropriate_translations_table_and_id(
    table: Type[IntraHostVariant] | Type[Mutation] | str,
) -> (str, str):
    """
    :param table: IntraHostVariants or Mutations, or string table name
    :return: (translations table name, name of id col to join to table)
    """

    if table is IntraHostVariant or table == TableNames.intra_host_variants:
        return TableNames.intra_host_translations, ColumnNames.intra_host_variant_id
    elif table is Mutation or table == TableNames.cns_samples_by_allele:
        return TableNames.cns_samples_by_amino_acid, ColumnNames.mutation_id
    else:
        raise ValueError
