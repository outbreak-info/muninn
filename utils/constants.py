class Env:
    FLU_DB_PASSWORD = 'FLU_DB_PASSWORD'
    FLU_DB_SUPERUSER_PASSWORD = 'FLU_DB_SUPERUSER_PASSWORD'
    FLU_DB_USER = 'FLU_DB_USER'
    FLU_DB_PORT = 'FLU_DB_PORT'
    FLU_DB_HOST = 'FLU_DB_HOST'
    FLU_DB_DB_NAME = 'FLU_DB_DB_NAME'


CHANGE_PATTERN = r'^(\w+):([a-zA-Z])(\d+)([a-zA-Z\-+]+)'


class PhenotypeMetricAssayTypes:
    DMS = 'DMS'
    EVE = 'EVEscape'


class DefaultGffFeaturesByRegion:
    HA = 'HA:cds-XAJ25415.1'


class LineageSystemNames:
    usda_genoflu = 'usda_genoflu'
    freyja_demixed = 'freyja_demixed'
