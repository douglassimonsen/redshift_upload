import datetime
from typing import Dict, List


def date_func(x: str, _: Dict) -> bool:
    """Tests if the string is a valid date"""
    # Timestamp must be between 4713-01-01 BC and 5874897-12-31. Not implemented because it seems unnecessary
    if x == "":
        return True
    try:
        datetime.datetime.strptime(x, "%Y-%m-%d")
        # TODO implement min/max valid range
        return True
    except:  # noqa
        return False


def timestamptz_func(x: str, _: Dict) -> bool:
    """Tests if the string is a valid timestamptz"""
    # Timestamp must be between 4713-01-01 00:00:00 BC and 5874897-12-31 12:59:59. Not implemented because it seems unnecessary
    if x == "":
        return True
    for fmt in ("%Y-%m-%d %H:%M:%S%z", "%Y-%m-%d %H:%M:%S%f%z", "%Y-%m-%d %H:%M%z"):
        try:
            datetime.datetime.strptime(x, fmt)
            return True
        except:  # noqa
            pass
    return False


def timestamp_func(x: str, _: Dict) -> bool:
    """Tests if the string is a valid timestamp"""
    # Timestamp must be between 4713-01-01 00:00:00 BC and 5874897-12-31 12:59:59. Not implemented because it seems unnecessary
    if x == "":
        return True
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M:%S%f", "%Y-%m-%d %H:%M"):
        try:
            datetime.datetime.strptime(x, fmt)
            return True
        except:  # noqa
            pass
    return False


def smallint_func(x: str, _: Dict) -> bool:
    """Tests if the string is a valid smallint"""
    if x == "":
        return True
    x = x.rstrip("0").rstrip(".")
    try:
        y = int(x)
        assert -32768 <= y <= 32767
        return True
    except:  # noqa
        return False


def int_func(x: str, _: Dict) -> bool:
    """Tests if the string is a valid int"""
    if x == "":
        return True
    x = x.rstrip("0").rstrip(".")
    try:
        y = int(x)
        assert -2147483648 <= y <= +2147483647
        return True
    except:  # noqa
        return False


def bigint_func(x: str, _: Dict) -> bool:
    """Tests if the string is a valid bigint"""
    if x == "":
        return True
    x = x.rstrip("0").rstrip(".")
    # rstrip would take 1.1.0.0 -> 1.1, so we do it in two steps. Technically,
    # this would take 1234..0 -> 1234, but that's a problem for future me
    # The solution is to use removesuffix("."), but it was only added in 3.9 :(
    try:
        y = int(x)
        assert -9223372036854775808 <= y <= 9223372036854775807
        return True
    except:  # noqa
        return False


def double_precision_func(x: str, _: Dict) -> bool:
    """Tests if the string is a valid double precision"""
    if x == "":
        return True
    try:
        float(x)
        return True
    except:  # noqa
        return False


def boolean_func(x: str, _: Dict) -> bool:
    """Tests if the string is a valid boolean"""
    if x == "":
        return True
    bool_opts = ["0", "1", "true", "false"]
    return str(x).lower() in bool_opts


def varchar_func(x: str, type_info: Dict) -> bool:
    """Tests if the string is a string less than 65536 bytes"""
    row_len = len(str(x).encode("utf-8"))
    type_info["suffix"] = max(row_len, type_info["suffix"] or 1)
    return row_len < 65536


def timetz_func(x: str, _: Dict) -> bool:
    """Tests if the string is a valid timetz"""
    if x == "":
        return True
    try:
        datetime.datetime.strptime(x, "%H:%M:%S%z")
        return True
    except:  # noqa
        return False


def time_func(x: str, _: Dict) -> bool:
    """Tests if the string is a valid time"""
    if x == "":
        return True
    try:
        datetime.datetime.strptime(x, "%H:%M:%S")
        return True
    except:  # noqa
        return False


def not_implemented(x: str, _: Dict) -> bool:
    """Default function"""
    return False


DATATYPES = [
    {"type": "DATE", "func": date_func},  # date should come before timestamps
    {"type": "TIMESTAMPTZ", "func": timestamptz_func},
    {"type": "TIMESTAMP", "func": timestamp_func},
    {"type": "SMALLINT", "func": smallint_func},
    {"type": "INTEGER", "func": int_func},
    {"type": "BIGINT", "func": bigint_func},
    {"type": "DOUBLE PRECISION", "func": double_precision_func},
    {"type": "BOOLEAN", "func": boolean_func},
    {"type": "TIMETZ", "func": timetz_func},
    {"type": "TIME", "func": time_func},
    {"type": "VARCHAR", "func": varchar_func},
]  # VARCHAR needs to go at the end, since it's the default. Otherwise TIME never happens :(
EXTRA_DATATYPES = [  # can be verified, but not automatically discovered
    {"type": "GEOMETRY", "func": not_implemented},
    {"type": "HLLSKETCH", "func": not_implemented},
    {"type": "CHAR", "func": not_implemented},
    {"type": "DECIMAL", "func": not_implemented},
    {"type": "REAL", "func": not_implemented},
]


def get_possible_data_types() -> List[Dict]:
    """Returns a dictionary of the possible datatypes, with a suffix in case there needs more specification (currently only used to house the length of varchars)"""
    return [{**dt, "suffix": None} for dt in DATATYPES]
