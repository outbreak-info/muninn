import re
from typing import Dict, Callable

from utils.constants import CHANGE_PATTERN


def get_value(
    row: Dict,
    key: str,
    allow_none: bool = False,
    blank_as_none: bool = True,
    transform: Callable | None = None
) -> any:
    try:
        v = row[key]
        if v == '' and blank_as_none:
            v = None
        if v is None and not allow_none:
            raise ValueError
        if transform is not None and v is not None:
            v = transform(v)
        return v
    except KeyError:
        if allow_none:
            return None
        else:
            raise ValueError


def bool_from_str(s: str) -> bool:
    return s.lower() == 'true'


def int_from_decimal_str(s: str) -> int:
    v = float(s)
    if not int(v) == v:
        raise ValueError
    return int(v)


def clean_up_gff_feature(gff_feature: str) -> str:
    """
    Strip extra stuff off of gff features
    HA:cds-XAJ25415.1  -->  XAJ25415.1
    cds-XAJ25415.1     -->  XAJ25415.1
    HA:XAJ25415.1      -->  XAJ25415.1
    :param gff_feature: gff feature that may have region and "cds" attached
    :return: just gff feature name with nothing else.
    """
    out = gff_feature
    if ':' in gff_feature:
        out = gff_feature.split(':')[1]
    out = out.replace('cds-', '')
    return out


def parse_change_string(change: str) -> (str, str, int, str):
    pattern = re.compile(CHANGE_PATTERN)
    match = pattern.fullmatch(change)

    if match is None:
        raise ValueError(f'This change string fails validation: {change}')

    region = match[1]
    ref = match[2]
    position = int(match[3])
    alt = match[4]

    return region, ref, position, alt
