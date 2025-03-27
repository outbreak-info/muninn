from typing import Dict, Callable


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
            raise KeyError


def bool_from_str(s: str) -> bool:
    return s.lower() == 'true'


def int_from_decimal_str(s: str) -> int:
    v = float(s)
    if not int(v) == v:
        raise ValueError
    return int(v)
