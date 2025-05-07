from functools import wraps
from time import time


def timing(f):
    @wraps(f)
    def wrap(*args, **kwargs):
        t0 = time()
        result = f(*args, **kwargs)
        t1 = time()
        print(f'{f.__name__.ljust(40, ' ')} took {t1 - t0}s')
        return result
    return wrap