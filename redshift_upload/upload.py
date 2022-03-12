try:
    from db_interfaces import redshift  # type: ignore
    from credential_store import credential_store
    import local_utilities  # type: ignore
    import redshift_utilities  # type: ignore
    import constants  # type: ignore
except ModuleNotFoundError:
    from .db_interfaces import redshift  # type: ignore
    from . import local_utilities
    from . import redshift_utilities
    from . import constants
    from .credential_store import credential_store
from typing import Dict, List
import logging
import time

log = logging.getLogger("redshift_utilities")


def upload(
    source: constants.SourceOptions = None,
    source_args: List = None,
    source_kwargs: Dict = None,
    column_types: Dict = None,
    schema_name: str = None,
    table_name: str = None,
    upload_options: Dict | None = None,
    user: str | None = None,
    log_level: str = "INFO",
    interface: redshift.Interface = None,
) -> redshift.Interface:
    """
    The main public function for uploading to redshift. Orchestrates the upload from start to finish.

    Upload option defaults are:
    "truncate_table": False,
    "drop_table": False,
    "cleanup_s3": True,
    "close_on_end": True,
    "grant_access": [],
    "diststyle": "even",
    "distkey": None,
    "sortkey": None,
    "load_in_parallel": None,
    "default_logging": True,
    "skip_checks": False,
    "stream_from_file": False,
    "skip_views": False,
    "node_count": 1,
    "default_timeout": 30 * 60 * 1000,  # 30 minutes
    "lock_timeout": 5 * 1000,  # 5 seconds
    "allow_alter_table": False,
    """
    start_time = time.time()
    source_args = source_args or []
    source_kwargs = source_kwargs or {}
    column_types = column_types or {}

    if user is None:
        log.info(
            "Since nothing was passed to parameter 'aws_info', using the default credentials"
        )
        aws_info = credential_store.credentials()
    elif isinstance(user, str):
        log.info(f"Using the stored credentials for user: {user}")
        aws_info = credential_store.credentials[user]
    elif isinstance(user, dict):
        log.info(f"Adding {user['db']['user']} to the credential store")
        credential_store.credentials.add(user)
        aws_info = credential_store.credentials[user["db"]["user"]]
    else:
        raise ValueError(
            "The user must either be a user in the credential store or a dictionary of credentials"
        )

    if schema_name is None:
        schema_name = aws_info["constants"].get("default_schema")

    upload_options, aws_info = local_utilities.check_coherence(
        schema_name, table_name, upload_options, aws_info
    )
    if upload_options["stream_from_file"] and not (
        isinstance(source, str) and source.endswith(".csv")
    ):
        raise ValueError(
            "The stream_from_file parameter only works when you supply a path to a CSV"
        )
    if upload_options["default_logging"]:
        local_utilities.initialize_logger(log_level)

    log.info("=" * 20)
    log.info(f"Beginning to upload table: {schema_name}.{table_name}")

    interface = interface or redshift.Interface(
        schema_name,
        table_name,
        aws_info,
        default_timeout=upload_options["default_timeout"],
        lock_timeout=upload_options["lock_timeout"],
    )
    if not interface.table_exists and upload_options["skip_checks"]:
        raise ValueError(
            "The table does not yet exist, you need the checks to determine what column types to use"
        )
    source = local_utilities.load_source(source, upload_options)
    if source.num_rows == 0:
        raise ValueError(
            "The source must have at least a single row to run this program"
        )

    if not upload_options["skip_checks"]:
        source.predefined_columns = redshift_utilities.get_defined_columns(
            column_types, interface, upload_options
        )
        local_utilities.fix_column_types(
            source, interface, upload_options["drop_table"]
        )

        if not upload_options["drop_table"] and interface.table_exists:
            redshift_utilities.compare_with_remote(source, upload_options, interface)
    else:
        log.info("Skipping data checks")

    if not upload_options["skip_views"] and interface.table_exists:
        redshift_utilities.log_dependent_views(interface)

    sources, load_in_parallel = local_utilities.chunkify(source, upload_options)
    interface.load_to_s3(sources)

    redshift_utilities.s3_to_redshift(
        interface, source.column_types, upload_options, source
    )
    if (
        not upload_options["skip_views"] and interface.table_exists
    ):  # still need to update those materialized views, so we can't check drop_table here. We can ignore normal views though!
        redshift_utilities.reinstantiate_views(
            interface, upload_options["drop_table"], upload_options["grant_access"]
        )
    if upload_options["cleanup_s3"] and source.num_rows > 0:
        interface.cleanup_s3(load_in_parallel)

    load_duration = round(time.time() - start_time, 2)
    log.info(
        f"Upload to {schema_name}.{table_name} finished in {load_duration} seconds!"
    )
    if aws_info["constants"]["logging_endpoint"]:
        local_utilities.post_data(
            aws_info["constants"]["logging_endpoint"],
            aws_info["constants"]["logging_endpoint_type"],
            interface,
            load_duration,
            source,
        )
    if upload_options["close_on_end"]:
        for conn in interface._db_conn.values():
            conn.close()
        del interface._s3_conn
    else:
        return interface
