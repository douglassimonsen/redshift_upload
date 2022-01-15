import numpy
import re
from typing import Dict, List, Union
import io
import pandas  # type: ignore
from mypy_boto3_s3 import Client
from psycopg2.extensions import connection  # type: ignore


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
    "cleanup_s3": True,
    "close_on_end": True,
    "grant_access": [],
    "diststyle": "even",
    "distkey": None,
    "sortkey": None,
    "load_in_parallel": None,  # count of parallel files
    "default_logging": True,
    "skip_checks": False,
    "stream_from_file": False,
    "skip_views": False,
    "node_count": 1,
    "default_timeout": 30 * 60 * 1000,  # 30 minutes
    "lock_timeout": 5 * 1000,  # 5 seconds
    "allow_alter_table": False,
}
DATETIME_FORMAT = "%Y-%m-%d %H:%M:%S.%f"
DATE_FORMAT = "%Y-%m-%d"
MAX_COLUMN_LENGTH = 63
MAX_THREAD_COUNT = 10
MAX_VARCHAR_LENGTH = (
    65535  # max limit in Redshift, as of 2020/03/27, but probably forever
)
varchar_len_re = re.compile(r"\((\d+)\)")
SourceOptions = Union[str, io.StringIO, List[Dict], pandas.DataFrame]
Connection = Union[Client, connection]
