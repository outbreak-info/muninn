from datetime import date

import dateparser


# todo: this method desperately needs to be unit tested.
def parse_collection_start_and_end(datestr: str) -> tuple[date, date]:
    """
    For reference on acceptable input formats see https://www.ncbi.nlm.nih.gov/biosample/docs/attributes/

    Technically, these dates can come with times attached, but I don't see us using those, so I'm throwing them away.

    :param datestr: String representing collection date for an SRA entry
    :return: tuple of two dates representing the start and end of the period of sample collection indicated by the input
    """
    set_base = {'DATE_ORDER': 'YMD', 'RETURN_AS_TIMEZONE_AWARE': False}
    set_full = set_base | {'STRICT_PARSING': True}
    set_month = set_base | {'REQUIRE_PARTS': ['year', 'month']}
    set_year = set_base | {'REQUIRE_PARTS': ['year']}
    set_first = {'PREFER_DAY_OF_MONTH': 'first', 'PREFER_MONTH_OF_YEAR': 'first'}
    set_last = {'PREFER_DAY_OF_MONTH': 'last', 'PREFER_MONTH_OF_YEAR': 'last'}

    raw_start_end = datestr.split('/')

    if len(raw_start_end) > 2:
        raise ValueError(f'Unable to parse: {datestr}')

    raw_start = raw_start_end[0]
    raw_end = raw_start_end[0]

    if len(raw_start_end) == 2:
        raw_end = raw_start_end[1]

    if (d0 := dateparser.parse(raw_start, settings=set_full)) is None:
        if (d0 := dateparser.parse(raw_start, settings=set_month | set_first)) is None:
            if (d0 := dateparser.parse(raw_start, settings=set_year | set_first)) is None:
                raise ValueError(f'Unable to parse begin date from: {datestr}')

    if (d1 := dateparser.parse(raw_end, settings=set_full)) is None:
        if (d1 := dateparser.parse(raw_end, settings=set_month | set_last)) is None:
            if (d1 := dateparser.parse(raw_end, settings=set_year | set_last)) is None:
                raise ValueError(f'Unable to parse end date from: {datestr}')

    return d0.date(), d1.date()
