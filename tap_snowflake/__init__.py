#!/usr/bin/env python3
# pylint: disable=missing-docstring,not-an-iterable,too-many-locals,too-many-arguments,too-many-branches,invalid-name,duplicate-code,too-many-statements

import collections
import copy
import itertools
import re
import sys
import logging

import singer
import singer.metrics as metrics
import singer.schema
import snowflake.connector
from singer import metadata
from singer import utils
from singer.catalog import Catalog, CatalogEntry
from singer.schema import Schema

import tap_snowflake.sync_strategies.common as common
import tap_snowflake.sync_strategies.full_table as full_table
import tap_snowflake.sync_strategies.incremental as incremental
from tap_snowflake.connection import SnowflakeConnection, SHOW_COMMAND_MAX_ROWS

LOGGER = singer.get_logger('tap_snowflake')


# Tone down snowflake connector logs noise
logging.getLogger('snowflake.connector').setLevel(logging.WARNING)

Column = collections.namedtuple('Column', [
    'table_catalog',
    'table_schema',
    'table_name',
    'column_name',
    'data_type',
    'character_maximum_length',
    'numeric_precision',
    'numeric_scale'])

REQUIRED_CONFIG_KEYS = [
    'account',
    'dbname',
    'warehouse',
    # 'tables'
]

# Snowflake data types
STRING_TYPES = set(['varchar', 'char', 'character', 'string', 'text'])
NUMBER_TYPES = set(['number', 'decimal', 'numeric'])
INTEGER_TYPES = set(['int', 'integer', 'bigint', 'smallint'])
FLOAT_TYPES = set(['float', 'float4', 'float8', 'real', 'double', 'double precision'])
DATETIME_TYPES = set(['datetime', 'timestamp', 'date', 'timestamp_ltz', 'timestamp_ntz', 'timestamp_tz'])
BINARY_TYPE = set(['binary', 'varbinary'])


def schema_for_column(c):
    '''Returns the Schema object for the given Column.'''
    data_type = c.data_type.lower()

    inclusion = 'available'
    result = Schema(inclusion=inclusion)

    if data_type == 'boolean':
        result.type = ['null', 'boolean']

    elif data_type in INTEGER_TYPES:
        result.type = ['null', 'number']

    elif data_type in FLOAT_TYPES:
        result.type = ['null', 'number']

    elif data_type in NUMBER_TYPES:
        result.type = ['null', 'number']

    elif data_type in STRING_TYPES or data_type in ["array", "object"]:
        result.type = ['null', 'string']
        # result.maxLength = c.character_maximum_length

    elif data_type in DATETIME_TYPES:
        result.type = ['null', 'string']
        result.format = 'date-time'

    elif data_type == 'time':
        result.type = ['null', 'string']
        result.format = 'time'

    elif data_type in BINARY_TYPE:
        result.type = ['null', 'string']
        result.format = 'binary'

    else:
        result = Schema(None,
                        inclusion='unsupported',
                        description='Unsupported data type {}'.format(data_type))
    return result


def create_column_metadata(cols):
    mdata = {}
    mdata = metadata.write(mdata, (), 'selected-by-default', False)
    for c in cols:
        schema = schema_for_column(c)
        mdata = metadata.write(mdata,
                               ('properties', c.column_name),
                               'selected-by-default',
                               schema.inclusion != 'unsupported')
        mdata = metadata.write(mdata,
                               ('properties', c.column_name),
                               'sql-datatype',
                               c.data_type.lower())

    return metadata.to_list(mdata)


def get_table_columns(snowflake_conn: SnowflakeConnection, tables: list):
    """Get column definitions of a list of tables

       It's using SHOW commands instead of INFORMATION_SCHEMA views because information_schemas views are slow
       and can cause unexpected exception of:
            Information schema query returned too much data. Please repeat query with more selective predicates.
    """
    table_columns = []
    for table in tables:
        queries = []

        LOGGER.info('Getting column information for %s...', table)

        # DEBUG: List all databases
        show_roles = 'SHOW ROLES'
        roles = snowflake_conn.query([show_roles], max_records=SHOW_COMMAND_MAX_ROWS)
        LOGGER.info(f'SHOW ROLES: {roles}')
        show_databases = 'SHOW DATABASES'
        databases = snowflake_conn.query([show_databases], max_records=SHOW_COMMAND_MAX_ROWS)
        LOGGER.info(f'SHOW DATABASES: {databases}')


        # Get column data types by SHOW commands
        show_columns = f'SHOW COLUMNS IN TABLE {table}'

        # Convert output of SHOW commands to tables and use SQL joins to get every required information
        select = snowflake_conn.get_data_type_query()
        queries.extend([show_columns, select])

        # Run everything in one transaction
        columns = snowflake_conn.query(queries, max_records=SHOW_COMMAND_MAX_ROWS)
        table_columns.extend(columns)

    return table_columns


def discover_catalog(snowflake_conn: SnowflakeConnection, config: dict, get_query_result=False):
    """Returns a Catalog describing the structure of the database."""
    tables = None
    temporary_tables = None
    if config.get('tables'):
        tables = config.get('tables').split(',')
    elif config.get('table_selection'):
        dbname = common.escape(config.get('dbname'))
        schema = common.escape(config.get('schema'))
        if config.get('table_selection'):
            if not tables:
                tables = []
            tables_from_config = [f"{dbname}.{schema}.{common.escape(t.get('name'))}" for t in config.get('table_selection')]
            tables.extend(tables_from_config)
        # we need to build it up database.schema.table
    elif config.get('queries'):
        if not tables:
            tables = []
        temporary_tables = []
        for query_info in config.get('queries'):
            query = query_info.get('query')
            table_name = query_info.get('name')
            replication_key_field = query_info.get('replication_key')
            start_date = config.get('start_date')
            replication_key_condition = f"{replication_key_field} > '{start_date}'" if start_date else "1=1"
            query = query.format(replication_key_condition = replication_key_condition)
            temporary_tables.append(snowflake_conn.create_table(table_name, query, limit_query=not get_query_result))
        tables.extend(temporary_tables)
    
    # confirm warehouse exists and is active
    warehouses = snowflake_conn.query("SHOW WAREHOUSES;")
    warehouse = [wh for wh in warehouses if wh.get("name") == config.get("warehouse")]
    if not warehouse:
        raise Exception(f"Warehouse {config.get('warehouse')} doesn't exist")

    sql_columns = get_table_columns(snowflake_conn, tables)

    table_info = {}
    columns = []
    for sql_col in sql_columns:
        catalog = sql_col['TABLE_CATALOG']
        schema = sql_col['TABLE_SCHEMA']
        table_name = sql_col['TABLE_NAME']

        if catalog not in table_info:
            table_info[catalog] = {}

        if schema not in table_info[catalog]:
            table_info[catalog][schema] = {}

        table_info[catalog][schema][table_name] = {
            'row_count': sql_col.get('ROW_COUNT'),
            'is_view': sql_col.get('TABLE_TYPE') == 'VIEW'
        }

        columns.append(Column(
            table_catalog=catalog,
            table_schema=schema,
            table_name=table_name,
            column_name=sql_col['COLUMN_NAME'],
            data_type=sql_col['DATA_TYPE'],
            character_maximum_length=sql_col['CHARACTER_MAXIMUM_LENGTH'],
            numeric_precision=sql_col['NUMERIC_PRECISION'],
            numeric_scale=sql_col['NUMERIC_SCALE']
        ))

    entries = []
    for (k, cols) in itertools.groupby(columns, lambda c: (c.table_catalog, c.table_schema, c.table_name)):
        cols = list(cols)
        (table_catalog, table_schema, table_name) = k
        schema = Schema(type='object',
                        properties={c.column_name: schema_for_column(c) for c in cols})
        md = create_column_metadata(cols)
        md_map = metadata.to_map(md)

        md_map = metadata.write(md_map, (), 'database-name', table_catalog)
        md_map = metadata.write(md_map, (), 'schema-name', table_schema)

        if (
                table_catalog in table_info and
                table_schema in table_info[table_catalog] and
                table_name in table_info[table_catalog][table_schema]
        ):
            # Row Count of views returns NULL - Transform it to not null integer by defaults to 0
            row_count = table_info[table_catalog][table_schema][table_name].get('row_count', 0) or 0
            is_view = table_info[table_catalog][table_schema][table_name]['is_view']

            md_map = metadata.write(md_map, (), 'row-count', row_count)
            md_map = metadata.write(md_map, (), 'is-view', is_view)

            # get pk and rep_key for tables
            replication_key = None
            key_properties = None
            replication_method = "FULL_TABLE"
            table_selection_config_data = [table for table in config.get("table_selection", []) if table.get("name") == table_name]
            query_config_data = [query for query in config.get("queries", []) if query.get("name").lower() == table_name.lower()]
            if table_selection_config_data:
                table_selection_config_data = table_selection_config_data[0]
                replication_key = table_selection_config_data.get("replication_key")
                if replication_key:
                    replication_method = "INCREMENTAL"
                key_properties = table_selection_config_data.get("primary_key")
                if key_properties and isinstance(key_properties, str):
                    key_properties = [key_properties]
            elif query_config_data:
                query_config_data = query_config_data[0]
                replication_key = query_config_data.get("replication_key")
                key_properties = []
                if replication_key:
                    replication_method = "INCREMENTAL"
                    key_properties.append(replication_key)
                primary_key = query_config_data.get("primary_key")
                if primary_key and isinstance(primary_key, str):
                    key_properties.append(primary_key)
                if not key_properties:
                    key_properties = None
            else:
                LOGGER.info(f"No config data found for table {table_name}, primary key and replication key not set.")

            entry = CatalogEntry(
                table=table_name,
                stream=table_name,
                metadata=metadata.to_list(md_map),
                tap_stream_id=common.generate_tap_stream_id(table_catalog, table_schema, table_name),
                schema=schema,
                replication_key=replication_key,
                key_properties=key_properties,
                replication_method=replication_method)

            entries.append(entry)
    return Catalog(entries), temporary_tables


def do_discover(snowflake_conn, config):
    catalog, temporary_tables = discover_catalog(snowflake_conn, config)
    if temporary_tables:
        snowflake_conn.drop_temporary_tables_if_exists(temporary_tables)
    catalog.dump()


# pylint: disable=fixme
# TODO: Maybe put in a singer-db-utils library.
def desired_columns(selected, table_schema):
    """Return the set of column names we need to include in the SELECT.

    selected - set of column names marked as selected in the input catalog
    table_schema - the most recently discovered Schema for the table
    """
    all_columns = set()
    available = set()
    automatic = set()
    unsupported = set()

    for column, column_schema in table_schema.properties.items():
        all_columns.add(column)
        inclusion = column_schema.inclusion
        if inclusion == 'automatic':
            automatic.add(column)
        elif inclusion == 'available':
            available.add(column)
        elif inclusion == 'unsupported':
            unsupported.add(column)
        else:
            raise Exception('Unknown inclusion ' + inclusion)

    selected_but_unsupported = selected.intersection(unsupported)
    if selected_but_unsupported:
        LOGGER.warning(
            'Columns %s were selected but are not supported. Skipping them.',
            selected_but_unsupported)

    selected_but_nonexistent = selected.difference(all_columns)
    if selected_but_nonexistent:
        LOGGER.warning(
            'Columns %s were selected but do not exist.',
            selected_but_nonexistent)

    not_selected_but_automatic = automatic.difference(selected)
    if not_selected_but_automatic:
        LOGGER.warning(
            'Columns %s are primary keys but were not selected. Adding them.',
            not_selected_but_automatic)

    return selected.intersection(available).union(automatic)


def resolve_catalog(discovered_catalog, streams_to_sync):
    result = Catalog(streams=[])

    # Iterate over the streams in the input catalog and match each one up
    # with the same stream in the discovered catalog.
    for catalog_entry in streams_to_sync:
        catalog_metadata = metadata.to_map(catalog_entry.metadata)
        replication_key = catalog_entry.to_dict().get('replication_key')

        discovered_table = discovered_catalog.get_stream(catalog_entry.tap_stream_id)
        database_name = common.get_database_name(catalog_entry)

        if not discovered_table:
            LOGGER.warning('Database %s table %s was selected but does not exist',
                           database_name, catalog_entry.table)
            continue

        selected = {k for k, v in catalog_entry.schema.properties.items()
                    if common.property_is_selected(catalog_entry, k) or k == replication_key}

        # These are the columns we need to select
        columns = desired_columns(selected, discovered_table.schema)
        replication_method = catalog_entry.to_dict().get('replication_method')
        result.streams.append(CatalogEntry(
            tap_stream_id=catalog_entry.tap_stream_id,
            metadata=catalog_entry.metadata,
            stream=catalog_entry.tap_stream_id,
            table=catalog_entry.table,
            replication_key=replication_key,
            schema=Schema(
                type='object',
                properties={col: discovered_table.schema.properties[col]
                            for col in columns}
            ),
            replication_method=replication_method,
        ))

    return result


def get_streams(snowflake_conn, catalog, config, state):
    """Returns the Catalog of data we're going to sync for all SELECT-based
    streams (i.e. INCREMENTAL and FULL_TABLE that require a historical
    sync).

    Using the Catalog provided from the input file, this function will return a
    Catalog representing exactly which tables and columns that will be emitted
    by SELECT-based syncs. This is achieved by comparing the input Catalog to a
    freshly discovered Catalog to determine the resulting Catalog.

    The resulting Catalog will include the following any streams marked as
    "selected" that currently exist in the database. Columns marked as "selected"
    and those labled "automatic" (e.g. primary keys and replication keys) will be
    included. Streams will be prioritized in the following order:
      1. currently_syncing if it is SELECT-based
      2. any streams that do not have state
      3. any streams that do not have a replication method of LOG_BASED
    """
    discovered, temporary_tables = discover_catalog(snowflake_conn, config, get_query_result=True)

    # Filter catalog to include only selected streams
    # pylint: disable=unnecessary-lambda
    selected_streams = list(filter(lambda s: common.stream_is_selected(s), catalog.streams))
    streams_with_state = []
    streams_without_state = []

    for stream in selected_streams:
        stream_state = state.get('bookmarks', {}).get(stream.tap_stream_id)

        if not stream_state:
            streams_without_state.append(stream)
        else:
            streams_with_state.append(stream)

    # If the state says we were in the middle of processing a stream, skip
    # to that stream. Then process streams without prior state and finally
    # move onto streams with state (i.e. have been synced in the past)
    currently_syncing = singer.get_currently_syncing(state)

    # prioritize streams that have not been processed
    ordered_streams = streams_without_state + streams_with_state

    if currently_syncing:
        currently_syncing_stream = list(filter(
            lambda s: s.tap_stream_id == currently_syncing, streams_with_state))

        non_currently_syncing_streams = list(filter(lambda s: s.tap_stream_id != currently_syncing, ordered_streams))

        streams_to_sync = currently_syncing_stream + non_currently_syncing_streams
    else:
        # prioritize streams that have not been processed
        streams_to_sync = ordered_streams

    return resolve_catalog(discovered, streams_to_sync), temporary_tables


def write_schema_message(catalog_entry, bookmark_properties=None, snowflake_conn=None):
    key_properties = common.get_key_properties(catalog_entry, snowflake_conn)

    singer.write_message(singer.SchemaMessage(
        stream=catalog_entry.stream,
        schema=catalog_entry.schema.to_dict(),
        key_properties=key_properties,
        bookmark_properties=bookmark_properties
    ))


def do_sync_incremental(snowflake_conn, catalog_entry, state, columns, config={}):
    LOGGER.info('Stream %s is using incremental replication', catalog_entry.stream)

    replication_key = catalog_entry.to_dict().get('replication_key')

    config = snowflake_conn.connection_config
    if config.get("table_selection"):
        tables = config['table_selection']
        table = next((x for x in tables if x.get('name') == catalog_entry.table), None)

        if table:
            replication_key = table.get('replication_key')

    if not replication_key:
        raise Exception(f'Cannot use INCREMENTAL replication for table ({catalog_entry.stream}) without a replication '
                        f'key.')

    write_schema_message(catalog_entry=catalog_entry,
                         bookmark_properties=[replication_key],
                         snowflake_conn=snowflake_conn)

    incremental.sync_table(snowflake_conn, catalog_entry, state, columns, config)

    singer.write_message(singer.StateMessage(value=copy.deepcopy(state)))


def do_sync_full_table(snowflake_conn, catalog_entry, state, columns):
    LOGGER.info('Stream %s is using full table replication', catalog_entry.stream)

    write_schema_message(catalog_entry, snowflake_conn=snowflake_conn)

    stream_version = common.get_stream_version(catalog_entry.tap_stream_id, state)

    full_table.sync_table(snowflake_conn, catalog_entry, state, columns, stream_version)

    # Prefer initial_full_table_complete going forward
    singer.clear_bookmark(state, catalog_entry.tap_stream_id, 'version')

    state = singer.write_bookmark(state,
                                  catalog_entry.tap_stream_id,
                                  'initial_full_table_complete',
                                  True)

    singer.write_message(singer.StateMessage(value=copy.deepcopy(state)))


def sync_streams(snowflake_conn, catalog, state, config={}):
    for catalog_entry in catalog.streams:
        columns = list(catalog_entry.schema.properties.keys())

        if not columns:
            LOGGER.warning('There are no columns selected for stream %s, skipping it.', catalog_entry.stream)
            continue

        state = singer.set_currently_syncing(state, catalog_entry.tap_stream_id)

        # Emit a state message to indicate that we've started this stream
        singer.write_message(singer.StateMessage(value=copy.deepcopy(state)))

        md_map = metadata.to_map(catalog_entry.metadata)

        replication_method = snowflake_conn.connection_config.get("replication_method") or catalog_entry.to_dict().get('replication_method')

        database_name = common.get_database_name(catalog_entry, snowflake_conn)
        schema_name = common.get_schema_name(catalog_entry, snowflake_conn)

        with metrics.job_timer('sync_table') as timer:
            timer.tags['database'] = database_name
            timer.tags['table'] = catalog_entry.table
            LOGGER.info('Beginning to sync %s.%s.%s', database_name, schema_name, catalog_entry.table)

            if replication_method.lower() == 'incremental':
                do_sync_incremental(snowflake_conn, catalog_entry, state, columns, config)
            else:
                do_sync_full_table(snowflake_conn, catalog_entry, state, columns)

    state = singer.set_currently_syncing(state, None)
    singer.write_message(singer.StateMessage(value=copy.deepcopy(state)))


def do_sync(snowflake_conn, config, catalog, state):
    catalog, temporary_tables = get_streams(snowflake_conn, catalog, config, state)
    sync_streams(snowflake_conn, catalog, state)
    if temporary_tables:
        snowflake_conn.drop_temporary_tables_if_exists(temporary_tables)


def main_impl():
    args = utils.parse_args(REQUIRED_CONFIG_KEYS)

    snowflake_conn = SnowflakeConnection(args.config)

    if args.discover:
        do_discover(snowflake_conn, args.config)
    elif args.catalog:
        state = args.state or {}
        do_sync(snowflake_conn, args.config, args.catalog, state)
    elif args.properties:
        catalog = Catalog.from_dict(args.properties)
        state = args.state or {}
        do_sync(snowflake_conn, args.config, catalog, state)
    else:
        LOGGER.info('No properties were selected')


def main():
    try:
        main_impl()
    except Exception as exc:
        LOGGER.critical(exc)
        raise exc

if __name__=="__main__":
    main()
