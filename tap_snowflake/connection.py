#!/usr/bin/env python3
import re
from typing import Union, List, Dict

import requests
import backoff
import singer
import sys
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import serialization
import snowflake.connector
import time

LOGGER = singer.get_logger('tap_snowflake')

# Max number of rows that a SHOW SCHEMAS|TABLES|COLUMNS can return.
# If more than this number of rows returned then tap-snowflake will raise TooManyRecordsException
SHOW_COMMAND_MAX_ROWS = 9999


class TooManyRecordsException(Exception):
    """Exception to raise when query returns more records than max_records"""


def retry_pattern():
    """Retry pattern decorator used when connecting to snowflake
    """
    return backoff.on_exception(backoff.expo,
                                snowflake.connector.errors.OperationalError,
                                max_tries=5,
                                on_backoff=log_backoff_attempt,
                                factor=2)


def log_backoff_attempt(details):
    """Log backoff attempts used by retry_pattern
    """
    LOGGER.info('Error detected communicating with Snowflake, triggering backoff: %d try', details.get('tries'))


def validate_config(config):
    """Validate configuration dictionary"""
    errors = []
    required_config_keys = [
        'account',
        'dbname',
        'warehouse',
        # 'tables'
    ]

    # Check if mandatory keys exist
    for k in required_config_keys:
        if not config.get(k, None):
            errors.append(f'Required key is missing from config: [{k}]')

    possible_authentication_keys =  [
      'password',
      'private_key_path',
      'access_token'
    ]
    if not any(config.get(k, None) for k in possible_authentication_keys):
        errors.append(
            f'Required authentication key missing. Existing methods: {",".join(possible_authentication_keys)}')

    return errors


class SnowflakeConnection:
    """Class to manage connection to snowflake data warehouse"""

    def __init__(self, connection_config):
        """
        connection_config:      Snowflake connection details
        """
        self.connection_config = connection_config
        config_errors = validate_config(connection_config)
        if len(config_errors) == 0:
            self.connection_config = connection_config
        else:
            LOGGER.error('Invalid configuration:\n   * %s', '\n   * '.join(config_errors))
            sys.exit(1)

    def get_private_key(self):
        """
        Get private key from the right location
        """
        if self.connection_config.get('private_key_path'):
            try:
                encoded_passphrase = self.connection_config['private_key_passphrase'].encode()
            except KeyError:
                encoded_passphrase = None

            with open(self.connection_config['private_key_path'], 'rb') as key:
                p_key= serialization.load_pem_private_key(
                        key.read(),
                        password=encoded_passphrase,
                        backend=default_backend()
                    )

            pkb = p_key.private_bytes(
                    encoding=serialization.Encoding.DER,
                    format=serialization.PrivateFormat.PKCS8,
                    encryption_algorithm=serialization.NoEncryption())
            return pkb

        return None

    def open_connection(self):
        """Connect to snowflake database"""
        return snowflake.connector.connect(
            user=self.connection_config['user'],
            password=self.connection_config.get('password', None),
            private_key=self.get_private_key(),
            account=self.connection_config['account'],
            database=self.connection_config['dbname'],
            warehouse=self.connection_config['warehouse'],
            schema=self.connection_config.get('schema', None),
            role=self.connection_config.get('role', None),
            insecure_mode=self.connection_config.get('insecure_mode', False),
            client_session_keep_alive=self.connection_config.get('client_session_keep_alive', True)
            # Use insecure mode to avoid "Failed to get OCSP response" warnings
            # insecure_mode=True
        )
    
    def refresh_token(self):
        """Connect to snowflake database"""
        payload = {
            "client_id": f'{self.connection_config["client_id"]}',
            "refresh_token": self.connection_config["refresh_token"],
            "grant_type": "refresh_token",
        }
        url = f"https://{self.connection_config['account']}.snowflakecomputing.com/oauth/token-request"
        auth = requests.auth.HTTPBasicAuth(self.connection_config["client_id"], self.connection_config["client_secret"])
        token_response = requests.post(url, data=payload, auth=auth)
        token_response = token_response.json()
        if token_response.get("error"):
            raise ConnectionError(token_response["message"])
        self.connection_config['access_token'] = token_response.get('access_token')

    def get_conn_creds(self):
        return dict(
                account=self.connection_config['account'],
                authenticator="oauth",
                token=self.connection_config['access_token'],
                warehouse=self.connection_config['warehouse'],
                database=self.connection_config['dbname'],
                schema=self.connection_config.get('schema', None),
                insecure_mode=self.connection_config.get('insecure_mode', False),
                role=self.connection_config.get('role', None),
                client_session_keep_alive=self.connection_config.get('client_session_keep_alive', True)
            )

    def open_connection_oauth(self):
        """Connect to snowflake database"""
        try:
            connector = self.get_conn_creds()
            return snowflake.connector.connect(**connector)
        except snowflake.connector.errors.DatabaseError as e:
            if "OAuth access token expired" in str(e):
                LOGGER.info("Access token expired, attempting to refresh.")
                self.refresh_token()
            connector = self.get_conn_creds()
            return snowflake.connector.connect(**connector)


    @retry_pattern()
    def connect_with_backoff(self):
        """Connect to snowflake database and retry automatically a few times if fails"""
        if self.connection_config.get('access_token'):
            return self.open_connection_oauth()
        return self.open_connection()

    def query(self, query: Union[List[str], str], params: Dict = None, max_records=0):
        """Run a query in snowflake"""
        result = []

        if params is None:
            params = {}
        else:
            if 'LAST_QID' in params:
                LOGGER.warning('LAST_QID is a reserved prepared statement parameter name, '
                               'it will be overridden with each executed query!')

        with self.connect_with_backoff() as connection:
            with connection.cursor(snowflake.connector.DictCursor) as cur:

                # Run every query in one transaction if query is a list of SQL
                if isinstance(query, list):
                    cur.execute('START TRANSACTION')
                    queries = query
                else:
                    queries = [query]

                qid = None

                for sql in queries:
                    LOGGER.debug('Running query: %s', sql)

                    # update the LAST_QID
                    params['LAST_QID'] = qid

                    #TEMPORARY TABLES NEED TO BE CREATED IN A TRANSACTION
                    cur.execute(sql, params)
                    qid = cur.sfqid

                    # Raise exception if returned rows greater than max allowed records
                    if 0 < max_records < cur.rowcount:
                        raise TooManyRecordsException(
                            f'Query returned too many records. This query can return max {max_records} records')

                    if cur.rowcount > 0:
                        result = cur.fetchall()

        return result

    def create_table(self, table_name, query, limit_query=True):
        '''
        Create a table and return the result of the query or the table structure
        limit_query: if True, limit the query to 0 rows, if False, do not add a limit to the query
        '''
        
        has_limit = re.search(r'\bLIMIT\s+\d+', query, re.IGNORECASE)

        if limit_query:
            if has_limit:
                # Replace the existing LIMIT clause with LIMIT 0
                query = re.sub(r'\bLIMIT\s+\d+', 'LIMIT 0', query, flags=re.IGNORECASE)
            else:
                # Append LIMIT 0 to the query. Do not execute the query only describe it
                query = query + " LIMIT 0"
        
        with self.connect_with_backoff() as connection:
            with connection.cursor(snowflake.connector.DictCursor) as cur:
                qid = None
                params = {}
                try:
                    cur.execute(f"SELECT 1 FROM {table_name}")
                    raise ValueError(f"Table {table_name} already exists - Please rename the query to a unique table name")
                except Exception as e:
                    LOGGER.info(f"Table {table_name} does not exist - Creating it")
                cur.execute(f"CREATE TABLE IF NOT EXISTS {table_name} AS ({query})")
                cur.execute(f'SHOW COLUMNS IN TABLE {table_name}')
                qid = cur.sfqid
                params = {'LAST_QID': qid}
                cur.execute(self.get_data_type_query(), params)
                if cur.rowcount > 0:
                    result = cur.fetchall()
                return table_name

    def drop_temporary_tables_if_exists(self, temporary_tables):
        with self.connect_with_backoff() as connection:
            with connection.cursor(snowflake.connector.DictCursor) as cur:
                for table in temporary_tables:
                    cur.execute(f"DROP TABLE IF EXISTS {table}")

    def get_data_type_query(self):
        return """
            WITH
              show_columns  AS (SELECT * FROM TABLE(RESULT_SCAN(%(LAST_QID)s)))
            SELECT show_columns."database_name"     AS table_catalog
                  ,show_columns."schema_name"       AS table_schema
                  ,show_columns."table_name"        AS table_name
                  ,show_columns."column_name"       AS column_name
                  -- ----------------------------------------------------------------------------------------
                  -- Character and numeric columns display their generic data type rather than their defined
                  -- data type (i.e. TEXT for all character types, FIXED for all fixed-point numeric types,
                  -- and REAL for all floating-point numeric types).
                  --
                  -- Further info at https://docs.snowflake.net/manuals/sql-reference/sql/show-columns.html
                  -- ----------------------------------------------------------------------------------------
                  ,CASE PARSE_JSON(show_columns."data_type"):type::varchar
                     WHEN 'FIXED' THEN 'NUMBER'
                     WHEN 'REAL'  THEN 'FLOAT'
                     ELSE PARSE_JSON("data_type"):type::varchar
                   END data_type
                  ,PARSE_JSON(show_columns."data_type"):length::number      AS character_maximum_length
                  ,PARSE_JSON(show_columns."data_type"):precision::number   AS numeric_precision
                  ,PARSE_JSON(show_columns."data_type"):scale::number       AS numeric_scale
              FROM show_columns
        """