import re
import pandas
import pytz
import datetime


def date_func(x):
    try:
        x = datetime.datetime.strptime(x, '%Y-%m-%d')
        # TODO implement min/max valid range
        return True
    except:
        return False


def timestamptz_func(x):
    try:
        x = datetime.datetime.strptime(x, '%Y-%m-%d %H:%M:%S%z')
        return True
    except:
        return False


def timestamp_func(x):
    try:
        x = datetime.datetime.strptime(x, '%Y-%m-%d %H:%M:%S')
        return True
    except:
        return False


def smallint_func(x):
    try:
        x = int(x)
        assert -32768 <= x <= 32767
        return True
    except:
        return False


def int_func(x):
    try:
        x = int(x)
        assert -2147483648 <= x <= +2147483647
        return True
    except:
        return False


def bigint_func(x):
    try:
        x = int(x)
        assert -9223372036854775808 <= x <= 9223372036854775807
        return True
    except:
        return False


def double_precision_func(x):
    try:
        float(x)
        return True
    except:
        return False


def boolean_func(x):
    bool_opts = [
        '0',
        '1',
        'True',
        'False'
    ]
    return str(x) in bool_opts


def varchar_func(x):
    return len(str(x).encode("utf-8")) < 65536


def not_implemented(x):
    return False


DATATYPES = {
    'DATE': date_func,  # date should come before timestamps
    'TIMESTAMPTZ': timestamptz_func,
    'TIMESTAMP': timestamp_func,
    'SMALLINT': smallint_func,
    'INTEGER': int_func,
    'BIGINT': bigint_func,
    'DOUBLE PRECISION': double_precision_func,
    'BOOLEAN': boolean_func,
    'VARCHAR': varchar_func,
    # 'TIME': not_implemented,
    # 'TIMETZ': not_implemented,
}
EXTRA_DATATYPES = {  # can be verified, but not automatically discovered
    'GEOMETRY': not_implemented,
    'HLLSKETCH': not_implemented,
    'CHAR': not_implemented,
    'DECIMAL': not_implemented,
    'REAL': not_implemented,
}


def get_possible_data_types():
    return list(DATATYPES.items())