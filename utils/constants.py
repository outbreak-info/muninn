import re
from datetime import datetime
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
COMMA_SEP_WORDLIKE_PATTERN = re.compile(r'(\w+,)*\w+')
# these dates are "simple" b/c they are a single timestamp and not null
SIMPLE_DATE_FIELDS = {'release_date', 'creation_date'}
# Unlike the simple dates, collection date is a range, and may be null
COLLECTION_DATE = 'collection_date'
LINEAGE = 'lineage'
DEFAULT_MAX_SPAN_DAYS = 366
DEFAULT_DAYS = 5


class PhenotypeMetricAssayTypes:
    DMS = 'DMS'
    EVE = 'EVEscape'


class DefaultGffFeaturesByRegion:
    HA = 'cds-XAJ25415.1'


class LineageSystemNames:
    usda_genoflu = 'usda_genoflu'
    freyja_demixed = 'freyja_demixed'


class DateBinOpt(Enum):
    month = 'month'
    week = 'week'
    day = 'day'

    def __init__(self, value):
        self._format_fn = None
        match value:
            case 'month':
                self._format_fn = format_iso_month
            case 'week':
                self._format_fn = format_iso_week
            case 'day':
                self._format_fn = format_iso_interval

    def __str__(self):
        return str(self.value)

    def format_iso_chunk(
        self,
        a: int | datetime,
        b: int | datetime
    ):
        return self._format_fn(a, b)


class NtOrAa(Enum):
    nt = 'nt'
    aa = 'aa'

    def __str__(self):
        return str(self.value)


class StandardColumnNames:
    sample_id = 'sample_id'
    allele_id = 'allele_id'
    amino_acid_substitution_id = 'amino_acid_substitution_id'
    # accession = 'accession'
    position_nt = 'position_nt'
    ref_nt = 'ref_nt'
    alt_nt = 'alt_nt'
    region = 'region'
    # gff_feature = 'gff_feature'
    # ref_codon = 'ref_codon'
    # alt_codon = 'alt_codon'
    # ref_aa = 'ref_aa'
    # alt_aa = 'alt_aa'
    # position_aa = 'position_aa'
