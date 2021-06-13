import datetime


def date_func(x, type_info):
    if x == '':
        return True
    try:
        x = datetime.datetime.strptime(x, '%Y-%m-%d')
        # TODO implement min/max valid range
        return True
    except:
        return False


def timestamptz_func(x, type_info):
    if x == '':
        return True
    try:
        x = datetime.datetime.strptime(x, '%Y-%m-%d %H:%M:%S%z')
        return True
    except:
        return False


def timestamp_func(x, type_info):
    if x == '':
        return True
    try:
        x = datetime.datetime.strptime(x, '%Y-%m-%d %H:%M:%S')
        return True
    except:
        return False


def smallint_func(x, type_info):
    if x == '':
        return True
    x = x.rstrip('0').rstrip('.')
    try:
        x = int(x)
        assert -32768 <= x <= 32767
        return True
    except:
        return False


def int_func(x, type_info):
    if x == '':
        return True
    x = x.rstrip('0').rstrip('.')
    try:
        x = int(x)
        assert -2147483648 <= x <= +2147483647
        return True
    except:
        return False


def bigint_func(x, type_info):
    if x == '':
        return True
    x = x.rstrip('0').rstrip('.')  # rstrip would take 1.1.0.0 -> 1.1, so we do it in two steps. Technically, this would take 1234..0 -> 1234, but that's a problem for future me
    try:
        x = int(x)
        assert -9223372036854775808 <= x <= 9223372036854775807
        return True
    except:
        return False


def double_precision_func(x, type_info):
    if x == '':
        return True
    try:
        float(x)
        return True
    except:
        return False


def boolean_func(x, type_info):
    if x == '':
        return True
    bool_opts = [
        '0',
        '1',
        'true',
        'false'
    ]
    return str(x).lower() in bool_opts


def varchar_func(x, type_info):
    row_len = len(str(x).encode("utf-8"))
    type_info['suffix'] = max(row_len, type_info['suffix'] or 1)
    return row_len < 65536


def not_implemented(x, type_info):
    return False


DATATYPES = [
    {'type': 'DATE', 'func': date_func},  # date should come before timestamps
    {'type': 'TIMESTAMPTZ', 'func': timestamptz_func},
    {'type': 'TIMESTAMP', 'func': timestamp_func},
    {'type': 'SMALLINT', 'func': smallint_func},
    {'type': 'INTEGER', 'func': int_func},
    {'type': 'BIGINT', 'func': bigint_func},
    {'type': 'DOUBLE PRECISION', 'func': double_precision_func},
    {'type': 'BOOLEAN', 'func': boolean_func},
    {'type': 'VARCHAR', 'func': varchar_func},
    # {'type': 'TIME', 'func': not_implemented},
    # {'type': 'TIMETZ', 'func': not_implemented},
]
EXTRA_DATATYPES = [  # can be verified, but not automatically discovered
    {'type': 'GEOMETRY', 'func': not_implemented},
    {'type': 'HLLSKETCH', 'func': not_implemented},
    {'type': 'CHAR', 'func': not_implemented},
    {'type': 'DECIMAL', 'func': not_implemented},
    {'type': 'REAL', 'func': not_implemented},
]


def get_possible_data_types():
    return [
        {**dt, 'suffix': None}
        for dt in DATATYPES
    ]