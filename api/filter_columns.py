"""
The column names a filter can use, read from the schema's DDL (DB/structure/sql/<table>/create_table_<table>.sql)
at import, so they follow the schema. Which of these each filter joins is declared at its filter_query() call.
"""
from pathlib import Path

SQL_DIR = Path(__file__).resolve().parent.parent / 'DB' / 'structure' / 'sql'


def columns(label: str, *tables: str) -> str:
    """'label: col, col, ...' for the tables' columns, leaving out id (ambiguous wherever tables are joined)."""
    names = []
    for table in tables:
        sql = (SQL_DIR / table / f'create_table_{table}.sql').read_text()
        for line in sql[sql.index('(') + 1:sql.rindex(')')].splitlines():
            name = line.split('--', 1)[0].split()[:1]
            if name and name[0] != 'id':
                names.append(name[0])
    return f'{label}: {", ".join(names)}'


SAMPLE_COLUMNS = columns('samples', 'samples')
GEO_COLUMNS = columns('geo_locations', 'geo_locations')
LINEAGE_COLUMNS = columns('lineages', 'lineages', 'lineage_systems', 'samples_lineages')
ALLELE_COLUMNS = columns('alleles (change_bin=nt)', 'alleles')
AMINO_ACID_COLUMNS = columns('amino_acids (change_bin=aa)', 'amino_acids')
