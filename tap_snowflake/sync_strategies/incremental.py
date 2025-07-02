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
    replication_key = catalog_entry.to_dict().get('replication_key')

    replication_key_value = None
    if replication_key:
        replication_key_value = get_start_date(catalog_entry, state, config, replication_key)


    # Write State

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


    with snowflake_conn.connect_with_backoff() as open_conn:
        with open_conn.cursor() as cur:
            select_sql = get_select_sql(catalog_entry, columns, snowflake_conn, replication_key, replication_key_value)
            common.sync_query(cur,
                              catalog_entry,
                              state,
                              select_sql,
                              columns,
                              stream_version,
                              params={},
                              replication_method="INCREMENTAL")



def get_start_date(catalog_entry, state, config, replication_key):
    replication_key_state = singer.get_bookmark(state,
                                                catalog_entry.tap_stream_id,
                                                'replication_key')

    # Use config start date as rep key value
    replication_key_value = config.get("start_date")
    LOGGER.info(f"Got start_date from config {replication_key_value}")

    # If this replication key is stored in state, use that instead
    if replication_key == replication_key_state:
        replication_key_value = singer.get_bookmark(state,
                                                    catalog_entry.tap_stream_id,
                                                    'replication_key_value')
        LOGGER.info(f"Got start_date from state {replication_key_value}")
    else:
        state = singer.write_bookmark(state,
                                      catalog_entry.tap_stream_id,
                                      'replication_key',
                                      replication_key)
        state = singer.clear_bookmark(state, catalog_entry.tap_stream_id, 'replication_key_value')

    return replication_key_value
    



def get_select_sql(catalog_entry, columns, snowflake_conn, replication_key, replication_key_value):
    select_sql = common.generate_select_sql(catalog_entry, columns, snowflake_conn)

    if replication_key_value is not None:
        if catalog_entry.schema.properties[replication_key].format == 'date-time':
            replication_key_value = pendulum.parse(replication_key_value)

        # pylint: disable=duplicate-string-formatting-argument
        select_sql += ' WHERE "{}" > \'{}\' ORDER BY "{}" ASC'.format(
                replication_key,
                replication_key_value,
                replication_key)
        return select_sql
    
    if replication_key is not None:
        select_sql += ' ORDER BY "{}" ASC'.format(replication_key)
        return select_sql
