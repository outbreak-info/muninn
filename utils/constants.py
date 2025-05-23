import re
from datetime import datetime
from decimal import Decimal
from enum import Enum

from utils.dates_and_times import format_iso_month, format_iso_week, format_iso_interval


class Env:
    FLU_DB_PASSWORD = 'FLU_DB_PASSWORD'
    FLU_DB_SUPERUSER_PASSWORD = 'FLU_DB_SUPERUSER_PASSWORD'
    FLU_DB_USER = 'FLU_DB_USER'
    FLU_DB_PORT = 'FLU_DB_PORT'
    FLU_DB_HOST = 'FLU_DB_HOST'
    FLU_DB_DB_NAME = 'FLU_DB_DB_NAME'


CHANGE_PATTERN = r'^(\w+):([a-zA-Z])(\d+)([a-zA-Z\-+]+)'
WORDLIKE_PATTERN = re.compile(r'\w+')
SIMPLE_DATE_FIELDS = {'release_date', 'creation_date'}
COLLECTION_DATE = 'collection_date'
DEFAULT_MAX_SPAN_DAYS = 366
DEFAULT_DAYS = 5


class PhenotypeMetricAssayTypes:
    DMS = 'DMS'
    EVE = 'EVEscape'


class DefaultGffFeaturesByRegion:
    HA = 'HA:cds-XAJ25415.1'


class LineageSystemNames:
    usda_genoflu = 'usda_genoflu'
    freyja_demixed = 'freyja_demixed'


class DateBinOpt(Enum):
    month = 'month'
    week = 'week'
    day = 'day'

    def __init__(self, value):
        self.format_fn = None
        match value:
            case 'month':
                self.format_fn = format_iso_month
            case 'week':
                self.format_fn = format_iso_week
            case 'day':
                self.format_fn = format_iso_interval

    def __str__(self):
        return str(self.value)

    def format_iso_chunk(
        self,
        a: int | datetime,
        b: int | datetime
    ):
        return self.format_fn(a, b)


class NtOrAa(Enum):
    nt = 'nt'
    aa = 'aa'

    def __str__(self):
        return str(self.value)
