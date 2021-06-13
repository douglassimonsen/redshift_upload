try:
    from db_interfaces import redshift
    import local_utilities
    import redshift_utilities
    import constants
except ModuleNotFoundError:
    from .db_interfaces import redshift
    from . import local_utilities
    from . import redshift_utilities
    from . import constants
from typing import Dict, List
import logging
import time
log = logging.getLogger("redshift_utilities")


def upload(
    source: constants.SourceOptions=None,
    source_args: List=None,
    source_kwargs: Dict=None,
    column_types: Dict=None,
    schema_name: str=None,
    table_name: str=None,
    upload_options: Dict=None,
    aws_info: Dict=None,
    log_level: str="INFO",
    interface: redshift.Interface=None
) -> redshift.Interface:
    """
    The main public function for uploading to redshift. Orchestrates the upload from start to finish.
    """
    start_time = time.time()
    source_args = source_args or []
    source_kwargs = source_kwargs or {}
    column_types = column_types or {}
    upload_options, aws_info = local_utilities.check_coherence(schema_name, table_name, upload_options, aws_info)

    if upload_options['default_logging']:
        local_utilities.initialize_logger(log_level)

    log.info("=" * 20)
    log.info(f"Beginning to upload table: {schema_name}.{table_name}")

    interface = interface or redshift.Interface(schema_name, table_name, aws_info)
    if not interface.table_exists and upload_options['skip_checks']:
        raise ValueError("The table does not yet exist, you need the checks to determine what column types to use")
    source = local_utilities.load_source(source)

    if not upload_options['skip_checks']:
        source.column_types = redshift_utilities.get_defined_columns(column_types, interface, upload_options)
        local_utilities.fix_column_types(source, interface, upload_options['drop_table'])

        if not upload_options['drop_table'] and interface.table_exists:
            redshift_utilities.compare_with_remote(source, interface)
    else:
        log.info("Skipping data checks")

    if not upload_options['skip_views'] and interface.table_exists:
        redshift_utilities.log_dependent_views(interface)

    sources, load_in_parallel = local_utilities.chunkify(source, upload_options)
    interface.load_to_s3(sources)

    redshift_utilities.s3_to_redshift(interface, source.column_types, upload_options)
    if not upload_options['skip_views'] and interface.table_exists:  # still need to update those materialized views, so we can't check drop_table here
        redshift_utilities.reinstantiate_views(interface, upload_options['drop_table'], upload_options['grant_access'])
    if interface.aws_info.get("records_table") is not None:
        redshift_utilities.record_upload(interface, source)
    if upload_options['cleanup_s3']:
        interface.cleanup_s3(load_in_parallel)
    log.info(f"Upload to {schema_name}.{table_name} finished in {round(time.time() - start_time, 2)} seconds!")
    if upload_options["close_on_end"]:
        interface._db_conn.close()
        del interface._s3_conn
    else:
        return interface
