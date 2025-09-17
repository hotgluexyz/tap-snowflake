#!/usr/bin/env python3
# pylint: disable=duplicate-code

import pendulum
import singer
from singer import metadata
import tap_snowflake.sync_strategies.common as common

LOGGER = singer.get_logger('tap_snowflake')

BOOKMARK_KEYS = {'replication_key', 'replication_key_value', 'version'}

def sync_table(snowflake_conn, catalog_entry, state, columns, config={}):
    """Sync table incrementally"""
    common.whitelist_bookmark_keys(BOOKMARK_KEYS, catalog_entry.tap_stream_id, state)

    config = snowflake_conn.connection_config
    catalog_metadata = metadata.to_map(catalog_entry.metadata)
    stream_metadata = catalog_metadata.get((), {})

    replication_key_metadata = stream_metadata.get('replication-key')

    if config.get("table_selection"):
        tables = config['table_selection']
        table = [x for x in tables if x.get('name') == catalog_entry.table]

        if len(table) > 0:
            replication_key_metadata = table[0]['replication_key']

    replication_key_state = singer.get_bookmark(state,
                                                catalog_entry.tap_stream_id,
                                                'replication_key')

    replication_key_value = config.get("start_date")
    
    # For non-datetime replication keys, check if table has a start_value configured
    if config.get("table_selection"):
        tables = config['table_selection']
        table = [x for x in tables if x.get('name') == catalog_entry.table]
        if len(table) > 0 and table[0].get('start_value') is not None:
            replication_key_value = table[0]['start_value']
            
    LOGGER.info(f"Got start value from config {replication_key_value}")

    if replication_key_metadata == replication_key_state:
        replication_key_value = singer.get_bookmark(state,
                                                    catalog_entry.tap_stream_id,
                                                    'replication_key_value')
        LOGGER.info(f"Got start value from state {replication_key_value}")
    else:
        state = singer.write_bookmark(state,
                                    catalog_entry.tap_stream_id,
                                    'replication_key',
                                    replication_key_metadata)
        state = singer.clear_bookmark(state, catalog_entry.tap_stream_id, 'replication_key_value')

    stream_version = common.get_stream_version(catalog_entry.tap_stream_id, state)
    state = singer.write_bookmark(state,
                                catalog_entry.tap_stream_id,
                                'version',
                                stream_version)

    activate_version_message = singer.ActivateVersionMessage(
        stream=catalog_entry.stream,
        version=stream_version
    )

    singer.write_message(activate_version_message)

    select_sql = common.generate_select_sql(catalog_entry, columns, snowflake_conn)
    params = {}

    with snowflake_conn.connect_with_backoff() as open_conn:
        with open_conn.cursor() as cur:
            select_sql = common.generate_select_sql(catalog_entry, columns, snowflake_conn)
            params = {}
            if replication_key_value is not None:
                # Handle datetime replication keys
                if catalog_entry.schema.properties[replication_key_metadata].format == 'date-time':
                    replication_key_value = pendulum.parse(replication_key_value)

                    # For datetime, use quotes in SQL
                    select_sql += ' WHERE "{}" > \'{}\' ORDER BY "{}" ASC'.format(
                        replication_key_metadata,
                        replication_key_value,
                        replication_key_metadata)
                else:
                    # Handle integer/numeric replication keys (like BIGINT)
                    # For numeric values, don't use quotes in SQL
                    select_sql += ' WHERE "{}" > {} ORDER BY "{}" ASC'.format(
                        replication_key_metadata,
                        replication_key_value,
                        replication_key_metadata)
            elif replication_key_metadata is not None:
                select_sql += ' ORDER BY "{}" ASC'.format(replication_key_metadata)

            common.sync_query(cur,
                            catalog_entry,
                            state,
                            select_sql,
                            columns,
                            stream_version,
                            params,
                            replication_method="INCREMENTAL")
