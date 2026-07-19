from typing import Type, List

from sqlalchemy import text

from DB.engine import get_async_session
from DB.models import Mutation, IntraHostVariant
from api.models import RegionAndGffFeatureInfo
from utils.constants import TableNames, ColumnNames, DistinctValueField


async def get_gff_features() -> List[str]:
    async with get_async_session() as session:
        res = await session.execute(
            text(
                f'''
                select distinct {ColumnNames.gff_feature} from {TableNames.amino_acids}
                '''
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
    DistinctValueField.library_selection: (TableNames.samples, ColumnNames.library_selection),
    DistinctValueField.library_source: (TableNames.samples, ColumnNames.library_source),
    DistinctValueField.library_layout: (TableNames.samples, ColumnNames.library_layout),
    DistinctValueField.isolation_source: (TableNames.samples, ColumnNames.isolation_source),
    DistinctValueField.center_name: (TableNames.samples, ColumnNames.center_name),
    DistinctValueField.bio_project: (TableNames.samples, ColumnNames.bio_project),
    DistinctValueField.country_name: (TableNames.geo_locations, ColumnNames.country_name),
    DistinctValueField.admin1_name: (TableNames.geo_locations, ColumnNames.admin1_name),
    DistinctValueField.admin2_name: (TableNames.geo_locations, ColumnNames.admin2_name),
    DistinctValueField.admin3_name: (TableNames.geo_locations, ColumnNames.admin3_name),
    DistinctValueField.region: (TableNames.alleles, ColumnNames.region),
    DistinctValueField.gff_feature: (TableNames.amino_acids, ColumnNames.gff_feature),
    DistinctValueField.lineage_system_name: (TableNames.lineage_systems, ColumnNames.lineage_system_name),
}


async def get_distinct_values(field: DistinctValueField) -> List[str]:
    table, column = _DISTINCT_VALUE_SOURCES[field]
    async with get_async_session() as session:
        res = await session.execute(
            text(
                f'''
                select distinct {column} from {table} where {column} is not null order by {column}
                '''
            )
        )
    return [row[0] for row in res.all()]


async def get_region_and_gff_features(
    intermediate: Type[Mutation] | Type[IntraHostVariant],
) -> List['RegionAndGffFeatureInfo']:
    translations_table, translations_join_id = get_appropriate_translations_table_and_id(intermediate)
    async with get_async_session() as session:
        res = await session.execute(
            text(
                f'''
                select distinct gff_feature, region
                from {TableNames.amino_acids} aa
                inner join {translations_table} t on t.{ColumnNames.amino_acid_id} = aa.id
                inner join {intermediate.__tablename__} inter on inter.id = t.{translations_join_id}
                inner join {TableNames.alleles} a on a.id = inter.{ColumnNames.allele_id}
                '''
            )
        )
    return [RegionAndGffFeatureInfo(**row) for row in res.mappings().all()]


# eventually-do: this doesn't really belong here because it's not a database query, but for now this is fine.
def get_appropriate_translations_table_and_id(table: Type[IntraHostVariant] | Type[Mutation] | str) -> (str, str):
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
