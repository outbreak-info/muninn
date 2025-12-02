import asyncio
from typing import Set

import polars as pl
import yaml

from DB.inserts.file_parsers.file_parser import FileParser
from DB.inserts.lineage_children import batch_delete_lineage_children, copy_insert_lineage_children, \
    get_all_lineages_immediate_children_by_system_as_pl_df
from DB.inserts.lineage_systems import find_or_insert_lineage_system
from DB.inserts.lineages import copy_insert_lineages, get_all_lineages_by_lineage_system_as_pl_df
from DB.models import LineageSystem
from utils.constants import StandardColumnNames, LineageSystemNames

PARENT_NAME = 'parent_name'
CHILD_NAME = 'child_name'


class FreyjaDemixedLineageHierarchyYamlParser(FileParser):

    def __init__(self, filename: str):
        self.filename = filename

    async def parse_and_insert(self):

        lineage_system_id = await find_or_insert_lineage_system(
            LineageSystem(lineage_system_name=LineageSystemNames.freyja_demixed)
        )

        # lineage_id, lineage_name
        existing_lineages = await get_all_lineages_by_lineage_system_as_pl_df(LineageSystemNames.freyja_demixed)

        relationships = self.extract_relationships()

        # for now we just strip out stars
        relationships = relationships.select(
            pl.col(PARENT_NAME).str.strip_chars_end('*'),
            pl.col(CHILD_NAME).str.strip_chars_end('*'),
        ).unique()

        # Find lineages that need to be added to DB
        missing_lineages = (
            relationships
            .select(PARENT_NAME)
            .rename({PARENT_NAME: StandardColumnNames.lineage_name})
            .join(
                existing_lineages.select(StandardColumnNames.lineage_name),
                how='anti',
                on=StandardColumnNames.lineage_name
            )
            .vstack(
                relationships
                .select(CHILD_NAME)
                .rename({CHILD_NAME: StandardColumnNames.lineage_name})
                .join(
                    existing_lineages.select(StandardColumnNames.lineage_name),
                    how='anti',
                    on=StandardColumnNames.lineage_name
                )
            )
            .unique()
            .with_columns(
                pl.lit(lineage_system_id).alias(StandardColumnNames.lineage_system_id)
            )
        )

        lineages_added = await copy_insert_lineages(missing_lineages)
        print(f'New lineages added: {lineages_added}')

        # get updated lineages
        existing_lineages = await get_all_lineages_by_lineage_system_as_pl_df(LineageSystemNames.freyja_demixed)

        # add ids to relationships
        relationships = relationships.join(
            existing_lineages,
            how='left',
            left_on=pl.col(PARENT_NAME),
            right_on=pl.col(StandardColumnNames.lineage_name)
        ).rename(
            {
                StandardColumnNames.lineage_id: StandardColumnNames.parent_id
            }
        ).join(
            existing_lineages,
            how='left',
            left_on=pl.col(CHILD_NAME),
            right_on=pl.col(StandardColumnNames.lineage_name)
        ).rename(
            {
                StandardColumnNames.lineage_id: StandardColumnNames.child_id
            }
        ).unique()

        # we assume that each time we ingest the file, we are getting the full, correct hierarchy
        # so we add any new relationships, and remove any that aren't in the new data.
        existing_relationships = await get_all_lineages_immediate_children_by_system_as_pl_df(LineageSystemNames.freyja_demixed)

        # find any existing relationships that should be dropped
        # this should be done before new relationships are added to avoid possible cycles
        dropped_relationships = existing_relationships.join(
            relationships,
            how='anti',
            on=[StandardColumnNames.parent_id, StandardColumnNames.child_id]
        )
        print(f'Deleting {len(dropped_relationships)} defunct relationships.')
        await batch_delete_lineage_children(dropped_relationships)

        # filter out the existing relationships, add in the new ones
        new_relationships = relationships.join(
            existing_relationships,
            how='anti',
            on=[StandardColumnNames.parent_id, StandardColumnNames.child_id]
        )
        relationships_added = await copy_insert_lineage_children(new_relationships)
        print(f'New relationships added: {relationships_added}')

        print(relationships)  # rm

    def extract_relationships(self):
        def make_pc_entry(parent_name: str, child_name: str) -> dict:
            return {PARENT_NAME: parent_name, CHILD_NAME: child_name}

        with open(self.filename, 'r') as f:
            hierarchy_input = yaml.safe_load(f)

        relationships = []
        for l in hierarchy_input:
            if 'parent' in l.keys():
                relationships.append(make_pc_entry(l['parent'], l['name']))
            if 'recombinant_parents' in l.keys():
                rps = l['recombinant_parents'].split(',')
                for rp in rps:
                    relationships.append(make_pc_entry(rp, l['name']))

        return pl.DataFrame(relationships)

    @classmethod
    def get_required_column_set(cls) -> Set[str]:
        return set("Expects a YAML file")
