import datetime
from typing import List

from utils.constants import DateBinOpt, COLLECTION_DATE

MID_COLLECTION_DATE = 'mid_collection_date'
MID_COLLECTION_DATE_CALCULATION = \
    '(collection_start_date + ((collection_end_date - collection_start_date) / 2))::date AS mid_collection_date'
YEAR = 'year'
CHUNK = 'chunk'
BIN_START = 'bin_start'
BIN_END = 'bin_end'


def get_extract_clause(group_by: str, date_bin: DateBinOpt, days: int) -> str:
    group_by_alias = group_by
    if group_by == COLLECTION_DATE:
        group_by_alias = MID_COLLECTION_DATE

    match date_bin:
        case DateBinOpt.week | DateBinOpt.month:
            return f'''
                extract({YEAR} from {group_by_alias}) as {YEAR},  
                extract({date_bin} from {group_by_alias}) as {CHUNK}
                '''
        case DateBinOpt.day:
            origin = datetime.date.today()
            return f'''
                date_bin('{days} days', {group_by_alias}, '{origin}') + interval '{days} days' as {BIN_END},
                date_bin('{days} days', {group_by_alias}, '{origin}') as {BIN_START}
                '''
        case _:
            raise NotImplementedError


def get_group_by_clause(
    date_bin: DateBinOpt,
    extra_cols: List[str] | None = None,
    prefix_cols: List[str] | None = None
) -> str:
    match date_bin:
        case DateBinOpt.week | DateBinOpt.month:
            cols = [YEAR, CHUNK]
        case DateBinOpt.day:
            cols = [BIN_START, BIN_END]
        case _:
            raise NotImplementedError

    if prefix_cols is not None:
        cols = prefix_cols + cols

    if extra_cols is not None:
        cols += extra_cols

    return f'group by {", ".join(cols)}'


def get_order_by_cause(date_bin: DateBinOpt) -> str:
    match date_bin:
        case DateBinOpt.week | DateBinOpt.month:
            return f'order by {YEAR}, {CHUNK}'
        case DateBinOpt.day:
            return f'order by {BIN_START}'
        case _:
            raise NotImplementedError
