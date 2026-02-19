#!/usr/bin/env python3
# pylint: disable=too-many-arguments,duplicate-code,too-many-locals

import copy
import datetime
import singer
import time
import re

import singer.metrics as metrics
from singer import metadata
from singer import utils
from pendulum import parse
import os
import json
import pathlib
from snowflake.connector import ProgrammingError
LOGGER = singer.get_logger('tap_snowflake')

def escape(string):
    """Escape strings to be SQL safe"""
    if '"' in string:
        raise Exception("Can't escape identifier {} because it contains a backtick"
                        .format(string))
    return '"{}"'.format(string)


def generate_tap_stream_id(catalog_name, schema_name, table_name):
    """Generate tap stream id as appears in properties.json"""
    return catalog_name + '_' + schema_name + '_' + table_name


def get_stream_version(tap_stream_id, state):
    """Get stream version from bookmark"""
    stream_version = singer.get_bookmark(state, tap_stream_id, 'version')

    if stream_version is None:
        stream_version = int(time.time() * 1000)

    return stream_version


def stream_is_selected(stream):
    """Detect if stream is selected to sync"""
    md_map = metadata.to_map(stream.metadata)
    selected_md = metadata.get(md_map, (), 'selected')

    return selected_md


def property_is_selected(stream, property_name):
    """Detect if field is selected to sync"""
    md_map = metadata.to_map(stream.metadata)
    return singer.should_sync_field(
        metadata.get(md_map, ('properties', property_name), 'inclusion'),
        metadata.get(md_map, ('properties', property_name), 'selected'),
        True)


def get_is_view(catalog_entry):
    """Detect if stream is a view"""
    md_map = metadata.to_map(catalog_entry.metadata)

    return md_map.get((), {}).get('is-view')


def get_database_name(catalog_entry, snowflake_conn=None):
    """Get database name from catalog"""
    md_map = metadata.to_map(catalog_entry.metadata)
    db = None
    if snowflake_conn:
        db = snowflake_conn.connection_config.get("database")

    return db or md_map.get((), {}).get('database-name')


def get_schema_name(catalog_entry, snowflake_conn=None):
    """Get schema name from catalog"""
    md_map = metadata.to_map(catalog_entry.metadata)
    if snowflake_conn:
        schema = snowflake_conn.connection_config.get("schema")

    return schema or md_map.get((), {}).get('schema-name')


def get_key_properties(catalog_entry, snowflake_conn=None):
    """Get key properties from catalog"""
    catalog_metadata = metadata.to_map(catalog_entry.metadata)
    stream_metadata = catalog_metadata.get((), {})
    if snowflake_conn:
        config = snowflake_conn.connection_config
        tables = config['table_selection']
        table = [x for x in tables if x.get('name') == catalog_entry.table]

        if len(table) > 0:
            primary_key = table[0].get('primary_key')
            if primary_key is not None:
                return [primary_key]

    is_view = get_is_view(catalog_entry)

    if is_view:
        key_properties = stream_metadata.get('view-key-properties', [])
    else:
        key_properties = stream_metadata.get('table-key-properties', [])

    return key_properties


def generate_select_sql(catalog_entry, columns, snowflake_conn=None):
    """Generate SQL to extract data froom snowflake"""
    database_name = get_database_name(catalog_entry, snowflake_conn)
    schema_name = get_schema_name(catalog_entry, snowflake_conn)
    escaped_db = escape(database_name)
    escaped_schema = escape(schema_name)
    escaped_table = escape(catalog_entry.table)
    escaped_columns = []

    for col_name in columns:
        escaped_col = escape(col_name)

        # fetch the column type format from the json schema alreay built
        property_format = catalog_entry.schema.properties[col_name].format

        # if the column format is binary, fetch the hexified value
        if property_format == 'binary':
            escaped_columns.append(f'hex_encode({escaped_col}) as {escaped_col}')
        else:
            escaped_columns.append(escaped_col)

    select_sql = f'SELECT {",".join(escaped_columns)} FROM {escaped_db}.{escaped_schema}.{escaped_table}'

    # escape percent signs
    select_sql = select_sql.replace('%', '%%')
    return select_sql

def format_datetime_to_iso_tuple(elem):
    value = (elem,)

    if isinstance(elem, datetime.datetime):
        value = (elem.isoformat() + '+00:00',)

    elif isinstance(elem, datetime.date):
        value = (elem.isoformat() + 'T00:00:00+00:00',)

    elif isinstance(elem, datetime.timedelta):
        epoch = datetime.datetime.utcfromtimestamp(0)
        timedelta_from_epoch = epoch + elem
        value = (timedelta_from_epoch.isoformat() + '+00:00',)

    elif isinstance(elem, datetime.time):
        value = (str(elem),)

    return value

# pylint: disable=too-many-branches
def row_to_singer_record(catalog_entry, version, row, columns, time_extracted):
    """Transform SQL row to singer compatible record message"""
    row_to_persist = ()
    for idx, elem in enumerate(row):
        property_type = catalog_entry.schema.properties[columns[idx]].type
        if isinstance(elem, datetime.datetime) or isinstance(elem, datetime.date) or isinstance(elem, datetime.timedelta) or isinstance(elem, datetime.time):
            row_to_persist += format_datetime_to_iso_tuple(elem)

        elif isinstance(elem, bytes):
            # for BIT value, treat 0 as False and anything else as True
            if 'boolean' in property_type:
                boolean_representation = elem != b'\x00'
                row_to_persist += (boolean_representation,)
            else:
                row_to_persist += (elem.hex(),)

        elif 'boolean' in property_type or property_type == 'boolean':
            if elem is None:
                boolean_representation = None
            elif elem == 0:
                boolean_representation = False
            else:
                boolean_representation = True
            row_to_persist += (boolean_representation,)

        else:
            row_to_persist += (elem,)
    rec = dict(zip(columns, row_to_persist))

    return singer.RecordMessage(
        stream=catalog_entry.stream,
        record=rec,
        version=version,
        time_extracted=time_extracted)


def whitelist_bookmark_keys(bookmark_key_set, tap_stream_id, state):
    """..."""
    for bookmark_key in [non_whitelisted_bookmark_key
                         for non_whitelisted_bookmark_key
                         in state.get('bookmarks', {}).get(tap_stream_id, {}).keys()
                         if non_whitelisted_bookmark_key not in bookmark_key_set]:
        singer.clear_bookmark(state, tap_stream_id, bookmark_key)


def clean_rep_key_value(rep_key_value):
    # If the value is numeric (int, float, or numeric string), return it as-is
    if isinstance(rep_key_value, (int, float)):
        return rep_key_value
    
    # If it's a string that represents a number, return it as-is
    if isinstance(rep_key_value, str):
        try:
            float(rep_key_value)
            return rep_key_value
        except ValueError:
            # Not a number, continue with datetime processing
            pass
    
    # Original datetime processing logic
    match = re.match(r'^(.*?)([\+\-]\d{2}:\d{2})([\+\-].*)?$', rep_key_value)

    if match:
        # If 2 or more timezones extract the first timezone
        datetime_part = match.group(1)
        first_timezone = match.group(2)

        # Reconstruct the datetime string with the first timezone offset
        cleaned_timestamp_str = datetime_part + first_timezone
    else:
        cleaned_timestamp_str = rep_key_value
    
    # check if rep_key_value is a valid datetime
    try:
        parse(cleaned_timestamp_str)
    except Exception as e:
        raise Exception(f"Failed while trying to parse rep_key_value {rep_key_value}, error: {e}")

    return cleaned_timestamp_str

def get_column_names(cursor, config, table_name):
    """Get column names from the table"""
    # Query to get column names
    column_query = f"""
    SELECT COLUMN_NAME, DATA_TYPE
    FROM INFORMATION_SCHEMA.COLUMNS 
    WHERE TABLE_SCHEMA = '{config['schema']}' 
    AND TABLE_NAME = '{table_name}'
    AND TABLE_CATALOG = '{config['dbname']}'
    ORDER BY ORDINAL_POSITION;
    """
    LOGGER.info(f"Column query: {column_query}")
    columns = cursor.execute(column_query)
    columns = columns.fetchall()
    column_types = {row[0]: row[1] for row in columns}
    return column_types

def cast_column_types(column_types, selected_columns):
    """Cast column types to the correct type"""
    formatted_column_names = []
    for column_name, column_type in column_types.items():
        if column_name not in selected_columns:
            continue
        if column_type == 'TIMESTAMP_LTZ' or column_type == 'TIMESTAMP_TZ':
            formatted_column_names.append(
                f"CAST(CONVERT_TIMEZONE('UTC', {column_name}) AS TIMESTAMP_NTZ) AS {column_name}"
            )        
        else:
            formatted_column_names.append(column_name)
    return formatted_column_names

def download_data_as_files(cursor, columns, config, catalog_entry, incremental_sql="", replication_key_metadata=None, state=None):
    """Download data as files"""

    aws_key = os.environ.get("AWS_ACCESS_KEY_ID")
    aws_secret_key = os.environ.get("AWS_SECRET_ACCESS_KEY")
    aws_session = os.environ.get("AWS_SESSION_TOKEN")
    aws_bucket = os.environ.get("ENV_ID")
    job_root = os.environ.get("JOB_ROOT")
    job_id = os.environ.get("JOB_ID", "")
    file_name = f"{config.get('dbname')}_{config.get('schema')}_{catalog_entry.table}"
    aws_export_path = f"s3://{aws_bucket}/{job_root}/sync-output"
    max_file_size = 5368709120 # 5GB
    local_output_dir = f"/home/hotglue/{job_id}/sync-output" if job_root else f"../.secrets"

    with cursor.connect_with_backoff() as open_conn:
        with open_conn.cursor() as cur:

            if replication_key_metadata is not None:
                cur.execute(f"SELECT MAX(\"{replication_key_metadata}\") FROM {config['dbname']}.{config['schema']}.{catalog_entry.table}")
                max_replication_key_value = cur.fetchone()[0]

            column_types = get_column_names(cur, config, catalog_entry.table)
            formatted_column_names = []

            LOGGER.info(f"Downloading file {file_name} to S3 {aws_export_path}")
            
            formatted_column_names = cast_column_types(column_types, columns)

            query_structure = f"""
            FROM (
                SELECT 
                    {', '.join(formatted_column_names)}
                FROM {config['dbname']}.{config['schema']}.{catalog_entry.table}
                {incremental_sql}
            )
            FILE_FORMAT = (TYPE = PARQUET COMPRESSION = SNAPPY)
            CREDENTIALS = (AWS_KEY_ID='{aws_key}' AWS_SECRET_KEY='{aws_secret_key}' AWS_TOKEN='{aws_session}')
            OVERWRITE = TRUE
            HEADER = TRUE
            MAX_FILE_SIZE = {max_file_size}
            """ 
            try:
                query = f"""
                COPY INTO '{aws_export_path}/{file_name}.parquet'
                {query_structure}
                SINGLE = TRUE
                """
                cur.execute(query)
            except ProgrammingError as e:
                if "Max file size" in str(e) and "exceeded for unload single file mode." in str(e):
                    # Fallback to multiple file mode
                    query = f"""
                    COPY INTO '{aws_export_path}/{file_name}'
                    {query_structure}
                    SINGLE = FALSE
                    """
                    cur.execute(query)
                else:
                    raise e from e
            
            LOGGER.info(f"File downloaded successfully to S3")
    
            # get rows len to add to the job metrics
            LOGGER.info(f"Getting job metrics for {file_name}")
            count_query = f"""
            SELECT *, COUNT(*) OVER() as rowcount FROM {config['dbname']}.{config['schema']}.{catalog_entry.table} {incremental_sql}
            """
            cur.execute(count_query)
            rowcount = cur._total_rowcount
            update_job_metrics(file_name, rowcount, local_output_dir)

            if replication_key_metadata is not None:
                state = singer.write_bookmark(state,
                                                catalog_entry.tap_stream_id,
                                                'replication_key',
                                                replication_key_metadata)

                rep_key_value = clean_rep_key_value(format_datetime_to_iso_tuple(max_replication_key_value)[0]) if max_replication_key_value is not None else None


                state = singer.write_bookmark(state,
                                                catalog_entry.tap_stream_id,
                                                'replication_key_value',
                                                rep_key_value)

            # download the file(s) from s3 to the local output directory
            LOGGER.info(f"Downloading file(s) for {file_name} to local output directory {local_output_dir}")
            error_file = os.path.join(local_output_dir, f"{file_name}_aws_s3_cp_errors.log")
            s3_cp_command = f"aws s3 cp {aws_export_path} {local_output_dir} --recursive 1>/dev/null 2>{error_file}"
            exitcode = os.system(s3_cp_command)
            
            if exitcode != 0:
                if os.path.exists(error_file):
                    with open(error_file, "r") as ef:
                        error_content = ef.read()
                    raise RuntimeError(f"Failed to download files from S3. aws s3 cp exit code: {exitcode}. Error: {error_content}")
                else:
                    raise RuntimeError(
                        f"Failed to download files from S3. aws s3 cp exit code: {exitcode}. "
                        f"Error output could not be captured (e.g. output directory missing or not writable): {local_output_dir}"
                    )
            LOGGER.info(f"File downloaded successfully to local output directory")

def sync_query(cursor, catalog_entry, state, select_sql, columns, stream_version, params, replication_method=None):
    """..."""
    replication_key = singer.get_bookmark(state,
                                          catalog_entry.tap_stream_id,
                                          'replication_key')

    time_extracted = utils.now()

    LOGGER.info('Running %s', select_sql)
    cursor.execute(select_sql, params)

    row = cursor.fetchone()
    rows_saved = 0

    database_name = get_database_name(catalog_entry)

    with metrics.record_counter(None) as counter:
        counter.tags['database'] = database_name
        counter.tags['table'] = catalog_entry.table

        while row:
            counter.increment()
            rows_saved += 1
            record_message = row_to_singer_record(catalog_entry,
                                                  stream_version,
                                                  row,
                                                  columns,
                                                  time_extracted)
            singer.write_message(record_message)

            md_map = metadata.to_map(catalog_entry.metadata)
            stream_metadata = md_map.get((), {})

            if replication_method is None:
                replication_method = stream_metadata.get('replication-method')

            if replication_method == 'FULL_TABLE':
                key_properties = get_key_properties(catalog_entry)

                max_pk_values = singer.get_bookmark(state,
                                                    catalog_entry.tap_stream_id,
                                                    'max_pk_values')

                if max_pk_values:
                    last_pk_fetched = {k:v for k, v in record_message.record.items()
                                       if k in key_properties}

                    state = singer.write_bookmark(state,
                                                  catalog_entry.tap_stream_id,
                                                  'last_pk_fetched',
                                                  last_pk_fetched)

            elif replication_method == 'INCREMENTAL':
                if replication_key is not None:
                    state = singer.write_bookmark(state,
                                                  catalog_entry.tap_stream_id,
                                                  'replication_key',
                                                  replication_key)

                    rep_key_value = clean_rep_key_value(record_message.record[replication_key])


                    state = singer.write_bookmark(state,
                                                  catalog_entry.tap_stream_id,
                                                  'replication_key_value',
                                                  rep_key_value)
            if rows_saved % 1000 == 0:
                singer.write_message(singer.StateMessage(value=copy.deepcopy(state)))

            row = cursor.fetchone()

    singer.write_message(singer.StateMessage(value=copy.deepcopy(state)))

def update_job_metrics(stream_name: str, record_count: int, output_dir: str):
    """
    Update metrics for a running job by tracking record counts per stream.

    This function maintains a JSON file that keeps track of the number of records
    processed for each stream during a job execution. The metrics are stored in
    a 'job_metrics.json' file in the specified folder path.

    Args:
        stream_name (str): The name of the stream being processed
        record_count (int): Number of records processed in the current batch
        output_dir (str): Folder path to store the job metrics

    Examples:
        >>> update_job_metrics("customers", 1000, "job_123")
        # Updates job_metrics.json with:
        # {
        #   "recordCount": {
        #     "customers": 1000
        #   }
        # }
    """
    job_metrics_path = os.path.expanduser(os.path.join(output_dir, "job_metrics.json"))

    if not os.path.isfile(job_metrics_path):
        pathlib.Path(job_metrics_path).touch()

    with open(job_metrics_path, "r+") as f:
        content = dict()

        try:
            content = json.loads(f.read())
        except:
            pass

        if not content.get("recordCount"):
            content["recordCount"] = dict()

        content["recordCount"][stream_name] = (
            content["recordCount"].get(stream_name, 0) + record_count
        )

        f.seek(0)
        LOGGER.info(f"Updating job metrics for {stream_name} with {record_count} records")
        f.write(json.dumps(content))