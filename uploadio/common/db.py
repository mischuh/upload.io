import copy
import logging
import sqlite3
from abc import abstractmethod
from collections import OrderedDict

from pandas import DataFrame

from .collections import get_value_from_dictionary
from typing import Dict, Any, Union

# from .convert import try_parse_int


class ConfigurationError(Exception):
    pass


class DatabaseConnectionInfo:
    def __init__(self) -> None:
        pass

    @classmethod
    def from_file(cls, connection_file) -> Any:
        import json
        with open(connection_file) as cur:
            conn = json.load(cur)
            return cls().parse(conn)

    @classmethod
    def from_dict(cls, kv: str) -> Any:
        return cls().parse(kv)

    @property
    def logger(self) -> logging.Logger:
        component = "{}.{}".format(type(self).__module__, type(self).__name__)
        return logging.getLogger(component)

    def copy(self) -> Any:
        """
        Returns a copy of this instance (shallow).
        :return: Returns a shallow copy of this instance.
        """
        return copy.copy(self)

    @abstractmethod
    def get_sqlalchemy_conn_str(self) -> str:
        raise NotImplementedError(
            "You have to implement this method in a subclass, dude!")

    @abstractmethod
    def parse(self, connection_json: Dict[str, Any]):
        """

        :param connection_json:
        :return:
        """
        raise NotImplementedError(
            "You have to implment this method in a subclass, dude!")

    @abstractmethod
    def execute_simple_statement(self, statement: str,
                                 is_non_query: bool=False,
                                 pandas_df: bool=False) -> Union[DataFrame, OrderedDict]:
        raise NotImplementedError(
            "You have to implement this method in a subclass, dude!")

    @staticmethod
    def _get_value_from_dict_or_die(d, *keys) -> Any:
        """keys: list of possible key candidates"""
        value = DatabaseConnectionInfo._get_value_from_dict_or_default(
            d, None, *keys)

        if value is None or not value:
            raise ConfigurationError("Failed to initialize database connection info. '{}' are not defined"
                                     .format(', '.join(keys)))

        return value

    @staticmethod
    def _get_value_from_dict_or_default(d, default, *keys) -> Any:
        """keys: list of possible key candidates"""
        # Probe our keys against the dictionary and memorize the result
        values = [get_value_from_dictionary(d, key, None) for key in keys]
        # Find next not None
        value = next((item for item in values if item is not None), 'not none')

        return value if value != 'not none' else default


class SqliteConnectionInfo(DatabaseConnectionInfo):
    def __init__(self) -> None:
        super().__init__()
        self.db_path = None

    def get_sqlalchemy_conn_str(self) -> str:
        return 'sqlite:///{}'.format(self.db_path)

    def execute_simple_statement(self,
                                 statement: str,
                                 is_non_query: bool=False,
                                 pandas_df: bool=False) -> Union[DataFrame, OrderedDict]:

        conn = sqlite3.connect(self.db_path)
        cur = conn.cursor()
        try:
            if not is_non_query:
                cur.execute(statement)

                rows = cur.fetchall()
                column_names = [desc[0] for desc in cur.description]

                if pandas_df:
                    return DataFrame(rows, columns=column_names)
                else:
                    res = []
                    for row in rows:
                        d = OrderedDict()
                        for i, col_name in zip(range(0, len(column_names)), column_names):
                            d[col_name] = row[i]
                        res.append(d)

                    return res
            else:
                cur.execute(statement)
                conn.commit()
                return None
        finally:
            cur.close()

    def parse(self, connection_json) -> Any:
        self.db_path = DatabaseConnectionInfo._get_value_from_dict_or_die(
            connection_json, 'db_path')
        return self

    def __str__(self):
        return self.get_sqlalchemy_conn_str()

    def __repr__(self):
        return "{}(db_path={})".format(self.__class__.__name__, repr(self.db_path))


# class PostgresConnectionInfo(DatabaseConnectionInfo):
#     PSYCOPG2_CONNECTION_TEMPLATE = "dbname = '{DB}' user = '{USER}' host = '{HOST}' password = '{PWD}' port = '{PORT}' sslmode = '{SSLMODE}' connect_timeout={CONNECT_TIMEOUT}"
#     SQLALCHEMY_CONNECTION_TEMPLATE = "postgresql://{USER}:{PWD}@{HOST}:{PORT}/{DB}?sslmode={SSLMODE}"
#     KEY = "bsd87dfghsfhjsdf"

#     def __init__(self):
#         """
#         Constructor.
#         """
#         super(PostgresConnectionInfo, self).__init__()
#         self.host = None
#         self.user = None
#         self.pwd = None
#         self.port = None
#         self.db = None
#         self.schema = None
#         self.is_parsed = False
#         # Default is sslmode = require
#         self.ssl_mode = 'require'
#         self.connect_timeout = None
#         self.query_timeout = None

#     def get_psycopg2_conn_str(self):
#         """
#         Returns the connection as a valid psycopg2 connection string.
#         :return:
#         """
#         conn_str = self._get_connection_string(self.PSYCOPG2_CONNECTION_TEMPLATE)
#         if self.query_timeout is not None and try_parse_int(self.query_timeout) is not None:
#             # In the configuration we specify query_timeout in the units of seconds, but ...
#             # ... psycopg2 actually wants it as milliseconds... So we multiply it with 1000.
#             additional = "options='-c statement_timeout={}s'".format(str(self.query_timeout))
#             conn_str += " " + additional

#         return conn_str

#     def get_sqlalchemy_conn_str(self):
#         """
#         Returns the connection as a valid sqlalchemy postgres connection string.
#         :return:
#         """
#         return self._get_connection_string(self.SQLALCHEMY_CONNECTION_TEMPLATE)

#     def execute_simple_statement(self, statement, is_non_query=False, pandas_df=False):
#         """
#         Executes a statement on the specified connection.
#         :param statement: statement to execute.
#         :param is_non_query: Specifies if this is a query (returns a result)
#         or not (e.g. create table -> returns nothing). You can also control if you do not want the result set of a
#         query. Makes sense for testing queries where the result rows doesn't matter.
#         :param pandas_df: A pandas DataFrame is returned when set to True; otherwise a list of ordered dictionaries.
#         :return:
#         """
#         conn_str = self.get_psycopg2_conn_str()

#         conn = psycopg2.connect(conn_str)
#         # psycopg2 opens automagically a transaction for us - we do not want that -> autocommit= True
#         # http://initd.org/psycopg/docs/connection.html#connection.autocommit
#         conn.autocommit = True
#         try:
#             cur = conn.cursor()
#             try:
#                 if not is_non_query:
#                     cur.execute(statement)

#                     rows = cur.fetchall()
#                     column_names = [desc[0] for desc in cur.description]

#                     if pandas_df:
#                         return DataFrame(rows, columns=column_names)
#                     else:
#                         res = []
#                         for row in rows:
#                             d = OrderedDict()
#                             for i, col_name in zip(range(0, len(column_names)), column_names):
#                                 d[col_name] = row[i]
#                             res.append(d)

#                         return res
#                 else:
#                     sql, vacuum = self.extract_vacuum_commands(statement.strip())
#                     if sql:
#                         cur.execute(sql)
#                     conn.commit()
#                     if vacuum is not None and len(vacuum) > 0:
#                         for v in vacuum:
#                             cur.execute(v)
#                             conn.commit()

#                     return None
#             finally:
#                 cur.close()
#         finally:
#             for notice in conn.notices:
#                 self.logger.info("[NOTICE]: " + notice)
#             conn.close()

#     @staticmethod
#     def extract_vacuum_commands(sql):
#         """Due to the fact that a vacuum command may not be executed with other commands or in a transaction block
#         we have to extract the vacuum command from the script and execute it isolated."""
#         starts = [m.start() for m in re.finditer('vacuum', sql.lower())]
#         if len(starts) == 0:
#             return sql, []
#         ends = [len(sql) if sql.find(';', start_pos) == -1 else sql.find(';', start_pos) for start_pos in starts]
#         start_end = list(zip(starts, ends))
#         vacuums = [sql[start_pos:end_pos+1] for start_pos, end_pos in start_end]
#         for vac in vacuums:
#             sql = sql.replace(vac, '')
#         return sql, vacuums

#     def parse(self, connection):
#         Argument.check_is_instance_of(connection, 'connection', dict)

#         self.host = DatabaseConnectionInfo._get_value_from_dict_or_die(connection, 'specific.host', 'host')
#         self.user = DatabaseConnectionInfo._get_value_from_dict_or_die(connection, 'specific.user', 'user')
#         self.pwd = DatabaseConnectionInfo._get_value_from_dict_or_die(connection, 'specific.pwd', 'pwd')
#         self.port = DatabaseConnectionInfo._get_value_from_dict_or_default(connection, 5432, 'specific.port', 'port')
#         self.db = DatabaseConnectionInfo._get_value_from_dict_or_die(connection, 'specific.database', 'database')
#         self.schema = DatabaseConnectionInfo._get_value_from_dict_or_die(connection, 'specific.schema', 'schema')
#         self.ssl_mode = DatabaseConnectionInfo._get_value_from_dict_or_default(connection, 'require',
#                                                                                'specific.sslmode', 'sslmode')
#         # Will force timeout when couldn't connect to database until the threshold is reached
#         # Timeout is in seconds
#         self.connect_timeout = DatabaseConnectionInfo._get_value_from_dict_or_default(
#             connection,
#             30,
#             'specific.connect_timeout',
#             'connect_timeout'
#         )
#         self.query_timeout = DatabaseConnectionInfo._get_value_from_dict_or_default(
#             connection,
#             None,
#             'specific.query_timeout',
#             'query_timeout'
#         )

#         # Many configs unfortunately use the typo. We have to keep the typo for quite amount of time
#         is_encrypted = DatabaseConnectionInfo._get_value_from_dict_or_default(
#             connection, None, 'specific.is_encrpted', 'is_encrpted')
#         if is_encrypted is None:
#             # This one does probe for the non-typo is_encrypted
#             is_encrypted = DatabaseConnectionInfo._get_value_from_dict_or_default(
#                 connection, False, 'specific.is_encrypted', 'is_encrypted')

#         if is_encrypted:
#             self.pwd = CryptFactory.get(key=PostgresConnectionInfo.KEY).decrypt(self.pwd)

#         self.is_parsed = True

#         return self

#     def __str__(self):
#         """
#         The informal representation of this instance.
#         :return: Informal representation.
#         """
#         return PostgresConnectionInfo.SQLALCHEMY_CONNECTION_TEMPLATE.format(
#             HOST=self.host, USER=self.user, PWD='*' * len(self.pwd), DB=self.db, PORT=self.port, SSLMODE=self.ssl_mode)

#     def _get_connection_string(self, template):
#         if not self.is_parsed:
#             raise RuntimeError("No connection info was parsed so far.")

#         return template.format(
#             HOST=self.host, PORT=self.port, USER=self.user, PWD=str(self.pwd),
#             DB=self.db, SSLMODE=self.ssl_mode, CONNECT_TIMEOUT=str(self.connect_timeout)
#         )
