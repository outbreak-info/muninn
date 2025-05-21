import re
from datetime import date


def parse_collection_start_and_end(datestr: str) -> tuple[date, date]:
    """
    For reference on acceptable input formats see https://www.ncbi.nlm.nih.gov/biosample/docs/attributes/

    Technically, these dates can come with times attached, but I don't see us using those, so I'm throwing them away.

    :param datestr: String representing collection date for an SRA entry
    :return: tuple of two dates representing the start and end of the period of sample collection indicated by the input
    """

    raw_start_end = datestr.split('/')

    if len(raw_start_end) > 2:
        raise ValueError(f'Unable to parse: {datestr}')

    raw_start = raw_start_end[0]
    # Discard any time
    raw_start = re.split('[T ]', raw_start)[0]

    start_parts = raw_start.split('-')
    year = int(start_parts[0])
    month = 1
    if len(start_parts) >= 2:
        month = int(start_parts[1])
    day = 1
    if len(start_parts) == 3:
        day = int(start_parts[2])
    if len(start_parts) > 3:
        raise

    d0 = date(year, month, day)

    raw_end = raw_start_end[0]
    if len(raw_start_end) == 2:
        raw_end = raw_start_end[1]

    raw_end = re.split('[T ]', raw_end)[0]

    end_parts = raw_end.split('-')
    year = int(end_parts[0])
    month = 12
    if len(end_parts) >= 2:
        month = int(end_parts[1])
    day = 31
    if len(end_parts) == 3:
        day = int(end_parts[2])
    if len(end_parts) > 3:
        raise

    d1 = date(year, month, day)

    return d0, d1


def format_iso_week(year: int, week: int):
    """
    Put year and week number into iso format.
    :param year: I will assume all our years are expressed in 4 digits already.
    If you are from <1000 or >9999 please contact the administrator.
    :param week:
    :return: (2025, 5) -> "2025-W05"
    """
    return f'{year}-W{str(week).rjust(2, "0")}'


def format_iso_month(year: int, month: int):
    return f'{year}-{str(month).rjust(2, "0")}'
