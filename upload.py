from db_interfaces import redshift
import local_utilities
import redshift_utilities
import constants
from typing import Dict, List


def upload(
    source: constants.SourceOptions=None,
    source_args: List=None,
    source_kwargs: Dict=None,
    column_types: Dict=None,
    schema_name: str=None,
    table_name: str=None,
    upload_options: Dict=None,
    aws_info: Dict=None,
):

    source_args = source_args or []
    source_kwargs = source_kwargs or {}
    column_types = column_types or {}
    upload_options, aws_info = local_utilities.check_coherence(upload_options, aws_info)

    interface = redshift.Interface(schema_name, table_name, aws_info)
    source = local_utilities.load_source(source, source_args, source_kwargs)

    column_types = redshift_utilities.get_defined_columns(source, column_types, interface, upload_options)
    source, column_types = local_utilities.fix_column_types(source, column_types, interface, upload_options['drop_table'])

    if not upload_options['drop_table'] and interface.table_exists:
        redshift_utilities.compare_with_remote(source, interface)

    if upload_options['drop_table'] and interface.table_exists:
        redshift_utilities.log_dependent_views(interface)

    interface.load_to_s3(source.to_csv(None, index=False, header=False, encoding="utf-8"))
    redshift_utilities.s3_to_redshift(interface, column_types, upload_options)
    if interface.table_exists:  # still need to update those materialized views, so we can't check drop_table here
        redshift_utilities.reinstantiate_views(interface, upload_options['drop_table'], upload_options['grant_access'])
    if interface.aws_info.get("records_table") is not None:
        redshift_utilities.record_upload(interface, source)
