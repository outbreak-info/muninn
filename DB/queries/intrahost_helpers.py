from utils.constants import ColumnNames


def get_alt_freq_where(min_freq: float, max_freq: float) -> tuple[str, dict]:
    highpass = f'(lower({ColumnNames.alt_freq_range}) >= :min_alt_freq)'
    lowpass = f'(upper({ColumnNames.alt_freq_range}) <= :max_alt_freq)'
    where = f'({" and ".join([highpass, lowpass])})'
    params = {
        'min_alt_freq': min_freq,
        'max_alt_freq': max_freq
    }
    return where, params