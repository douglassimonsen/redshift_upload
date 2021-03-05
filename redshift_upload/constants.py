import numpy
import re
from typing import Dict, List, Union
import pandas


NaT = numpy.datetime64("NaT")
DTYPE_MAPS = {
    "int64": "bigint",
    "float64": "double precision",
    "bool": "bool",
    "datetime64[ns]": "timestamp",
}
UPLOAD_DEFAULTS = {
    "truncate_table": False,
    "drop_table": False,
    "cleanup_s3": False,
    "grant_access": [],
    "diststyle": "even",
    "distkey": None,
    "sortkey": None,
    "load_in_parallel": 1,  # count of parallel files
    "default_logging": True,
    "skip_checks": False,
    "skip_views": False,
    "load_as_csv": False,
    "no_header": False,  # indicates the CSV has no header row at the beginning
}
DATETIME_FORMAT = "%Y-%m-%d %H:%M:%S.%f"
DATE_FORMAT = "%Y-%m-%d"
MAX_COLUMN_LENGTH = 63
MAX_THREAD_COUNT = 10
MAX_VARCHAR_LENGTH = 65535  # max limit in Redshift, as of 2020/03/27, but probably forever
varchar_len_re = re.compile(r"\((\d+)\)")
SourceOptions = Union[str, pandas.DataFrame, List[Dict]]
