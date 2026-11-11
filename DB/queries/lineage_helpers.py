import re

from utils.constants import ColumnNames, TableNames

WILDCARD = '*'

# Characters the filter tokenizer allows in a value (parser/tokenizer.py). Names that get inlined
# into SQL as literals (see get_lineage_filter_sql) must stick to these: no quotes, no backslashes.
_SAFE_LITERAL = re.compile(r'[\w\-:\. ]+')


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


def _lineage_ids_sql(name_sql: str | None, system_sql: str | None, include_descendants: bool) -> str:
    """
    SELECT returning (lineage_id, lineage_system_id) for lineages whose name equals the SQL
    expression `name_sql` and whose system name equals `system_sql` (either may be None for no
    restriction), plus all of their descendants when `include_descendants`. The expressions are
    spliced in as-is, so they must be bind params or already-safe literals.
    """
    conditions = []
    if name_sql is not None:
        conditions.append(f'l.{ColumnNames.lineage_name} = {name_sql}')
    if system_sql is not None:
        conditions.append(f'ls.{ColumnNames.lineage_system_name} = {system_sql}')
    where = f'where {" and ".join(conditions)}' if conditions else ''

    sql = f'''
        select l.id as lineage_id,
               l.{ColumnNames.lineage_system_id}
        from {TableNames.lineages} l
        inner join {TableNames.lineage_systems} ls on ls.id = l.{ColumnNames.lineage_system_id}
        {where}
        '''

    if include_descendants:
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

    return sql


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
    params = {}
    is_wildcard = False
    name_sql = None
    system_sql = None
    if lineage_name is not None:
        base_name, is_wildcard = parse_lineage_name(lineage_name)
        name_sql = ':lineage_name'
        params['lineage_name'] = base_name
    if lineage_system_name is not None:
        system_sql = ':lineage_system_name'
        params['lineage_system_name'] = lineage_system_name

    return _lineage_ids_sql(name_sql, system_sql, is_wildcard), params


def get_lineage_filter_sql(lineage_name: str, negate: bool = False) -> str:
    """
    Self-contained WHERE predicate for the filter mini-language: `lineage_name = XFG*` becomes a
    match on (lineage_name, lineage_system_name) against XFG plus all its descendants, in every
    lineage system that has an XFG, each descendant paired with its own system. `negate` gives the
    `!=` form. Uses bare lineage_name / lineage_system_name columns, like any other filter term, and
    inlines the name as a literal (the parser's output carries no bind params), so the name must
    only contain the tokenizer's safe characters.
    """
    base_name, is_wildcard = parse_lineage_name(lineage_name)
    if not _SAFE_LITERAL.fullmatch(base_name):
        raise ValueError(f"Invalid lineage name '{lineage_name}'")

    ids_sql = _lineage_ids_sql(f"'{base_name}'", None, include_descendants=is_wildcard)
    # One line, since filter output ends up inline in other queries and in error messages. Only line
    # ends are trimmed: the quoted name sits mid-line, so its content is untouched.
    ids_sql = ' '.join(line.strip() for line in ids_sql.splitlines() if line.strip())
    op = 'not in' if negate else 'in'
    # Fully parenthesized: some call sites splice the filter in without parentheses.
    return (
        f'(({ColumnNames.lineage_name}, {ColumnNames.lineage_system_name}) {op} ('
        f'select fl.{ColumnNames.lineage_name}, fls.{ColumnNames.lineage_system_name} '
        f'from {TableNames.lineages} fl '
        f'inner join {TableNames.lineage_systems} fls on fls.id = fl.{ColumnNames.lineage_system_id} '
        f'where fl.id in (select lineage_id from ({ids_sql}) lineage_ids)'
        f'))'
    )
