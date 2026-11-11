from utils.constants import ColumnNames, TableNames

WILDCARD = '*'


def parse_lineage_name(lineage_name: str) -> tuple[str, bool]:
    """
    Split a requested lineage name into (base name, is_wildcard). A single trailing '*' means
    "this lineage plus all of its descendants": 'XFG*' -> ('XFG', True), 'XFG' -> ('XFG', False).
    Raises ValueError for a '*' anywhere else, or for a name that is nothing but '*'.
    """
    is_wildcard = lineage_name.endswith(WILDCARD)
    base_name = lineage_name[:-1] if is_wildcard else lineage_name
    if not base_name or WILDCARD in base_name:
        raise ValueError(
            f"Invalid lineage name '{lineage_name}': '*' is only allowed once, as a trailing wildcard "
            f"after a lineage name (e.g. 'XFG*')"
        )
    return base_name, is_wildcard


def get_lineage_ids_query(
    lineage_name: str | None,
    lineage_system_name: str | None,
) -> tuple[str, dict]:
    """
    (sql, params) for a SELECT returning (lineage_id, lineage_system_id) of the named lineage, plus
    all of its descendants when the name ends in '*', optionally restricted to one lineage system.
    With both arguments None it selects every lineage. Meant to be spliced in as a CTE or subquery;
    the names are only ever passed as bind params (:lineage_name, :lineage_system_name).
    """
    conditions = []
    params = {}
    is_wildcard = False
    if lineage_name is not None:
        base_name, is_wildcard = parse_lineage_name(lineage_name)
        conditions.append(f'l.{ColumnNames.lineage_name} = :lineage_name')
        params['lineage_name'] = base_name
    if lineage_system_name is not None:
        conditions.append(f'ls.{ColumnNames.lineage_system_name} = :lineage_system_name')
        params['lineage_system_name'] = lineage_system_name
    where = f'where {" and ".join(conditions)}' if conditions else ''

    sql = f'''
        select l.id as lineage_id,
               l.{ColumnNames.lineage_system_id}
        from {TableNames.lineages} l
        inner join {TableNames.lineage_systems} ls on ls.id = l.{ColumnNames.lineage_system_id}
        {where}
        '''

    if is_wildcard:
        # `union` rather than `union all`: the deep-children view can list a child more than once
        # when it is reachable through several paths. Children always share their parent's system
        # (enforced by trigger), so the parent's system id is theirs too.
        sql += f'''
        union
        select ldc.{ColumnNames.child_id},
               l.{ColumnNames.lineage_system_id}
        from {TableNames.lineages_deep_children} ldc
        inner join {TableNames.lineages} l on l.id = ldc.{ColumnNames.parent_id}
        inner join {TableNames.lineage_systems} ls on ls.id = l.{ColumnNames.lineage_system_id}
        {where}
        '''

    return sql, params
