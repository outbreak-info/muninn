

def value_or_none(row, key, fn=None):
    try:
        v = row[key]
        if v == '':
            v = None
        elif fn is not None:
            return fn(v)
        return v
    except KeyError:
        return None
    except ValueError:
        return None
