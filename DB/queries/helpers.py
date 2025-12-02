from typing import Type, List

from sqlalchemy import text

from DB.engine import get_async_session
from DB.models import Mutation, IntraHostVariant
from api.models import RegionAndGffFeatureInfo
from utils.constants import TableNames, StandardColumnNames


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
                inner join {translations_table} t on t.{StandardColumnNames.amino_acid_id} = aa.id
                inner join {intermediate.__tablename__} inter on inter.id = t.{translations_join_id}
                inner join {TableNames.alleles} a on a.id = inter.{StandardColumnNames.allele_id}
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
        return TableNames.intra_host_translations, StandardColumnNames.intra_host_variant_id
    elif table is Mutation or table == TableNames.mutations:
        return TableNames.mutations_translations, StandardColumnNames.mutation_id
    else:
        raise ValueError
