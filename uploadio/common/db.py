import sqlite3
from abc import abstractmethod
from typing import Any, Dict, Iterable, Optional, Type

import psycopg2
from pandas import DataFrame

from src.p3common.common import validators as validate
from uploadio.utils import Loggable


class ConfigurationError(Exception):
    pass


class AbstractDatabase(Loggable):

    def __init__(self, connection: Dict[str, Any]) -> None:
        self.connection = connection

    def execute(self, statement: str, **options) -> Optional[DataFrame]:
        return self._execute(statement, **options)

    @abstractmethod
    def _execute(self, statement: str,
                 modify: bool=False, **options) -> Optional[DataFrame]:
        raise NotImplementedError()

    @abstractmethod
    def select(self, statement: str, **options) -> DataFrame:
        raise NotImplementedError()

    @abstractmethod
    def insert(self, *args, **options) -> None:
        raise NotImplementedError()

    @abstractmethod
    def update(self, **options) -> None:
        raise NotImplementedError()

    def __repr__(self) -> str:
        return "{}({})".format(
            self.__class__.__name__,
            self.__str__()
        )


class SQLAlchemyDatabase(AbstractDatabase):

    def __init__(self, connection: Dict[str, Any]) -> None:
        validate.is_in_dict_keys('uri', connection['connection'])
        validate.is_in_dict_keys('database', connection['connection'])
        super().__init__(connection)
        self.database = "{}/{}.db".format(
            self.connection['connection']['uri'], 
            self.connection['connection']['database']
        )
        # TODO: Validate config https://pypi.org/project/schema/
        self.table = self.connection['connection']['table']
        self.options = self.connection.get('options', {})

    def sqlalchemy_conn_str(self) -> str:
        return 'sqlite:///{}'.format(self.database)

    def _execute(self, statement: str, modify: bool=False,
                 values: Iterable=None, **options) -> Optional[DataFrame]:
        conn = sqlite3.connect(self.database)
        try:
            cur = conn.cursor()
            try:
                if not modify:
                    cur.execute(statement)
                    rows = cur.fetchall()
                    column_names = [desc[0] for desc in cur.description]
                    return DataFrame(rows, columns=column_names)
                else:
                    self.logger.debug(statement)
                    cur.execute(statement, [] if not values else values)
                    conn.commit()
            finally:
                cur.close()
        finally:
            conn.close()

    def select(self, statement: str, **options) -> DataFrame:
        return self._execute(statement, **options)

    def insert(self, upsert: bool=True,
               conflict_target: str=None, **data) -> None:
        
        columns = data['fields'].keys()
        values = [data['fields'][col]['value'] for col in columns]   

        statement = 'INSERT INTO {} ({}) VALUES ({})'.format(
            self.table, 
            ', '.join(map(lambda x: "'" + x + "'", columns)), 
            ', '.join('?' * len(values))
        )

        # requires sqlite3 >= 3.24.0
        if upsert:
            validate.not_none(conflict_target)
            statement = '{} ON CONFLICT({}) DO NOTHING'.format(
                statement,
                conflict_target
            )

        return self._execute(
            statement=statement,
            modify=True,
            values=values
        )

    def update(self, **options) -> None:
        pass

    def __str__(self) -> str:
        return self.sqlalchemy_conn_str()


class PostgresDatabase(AbstractDatabase):

    PSYCOPG2_CONNECTION_TEMPLATE = "dbname = '{DB}' user = '{USER}' " \
            "host = '{HOST}' password = '{PWD}' port = '{PORT}' " \
            "sslmode = '{SSLMODE}' connect_timeout={CONNECT_TIMEOUT}"

    def __init__(self, connection: Dict[str, Any]) -> None:
        super().__init__(connection)
        self.parsed = self._parse()

    def _parse(self) -> bool:
        # TODO: Schema validation is much more elegant and efficent
        self.host = self.connection['connection']['host']
        self.user = self.connection['connection']['user']
        self.pwd = self.connection['connection']['password']
        self.port = self.connection['connection']['port']
        self.db = self.connection['connection']['database']
        self.schema = self.connection['connection']['schema']
        self.table = self.connection['connection']['table']
        self.ssl_mode = self.connection['connection']['ssl']
        # Will force timeout when couldn't connect to database
        # until the threshold is reached. Timeout is in seconds
        self.connect_timeout = 30
        return True

    def _get_connection_string(self):
        template = self.PSYCOPG2_CONNECTION_TEMPLATE
        return template.format(
            HOST=self.host, PORT=self.port, USER=self.user, PWD=str(self.pwd),
            DB=self.db, SSLMODE=self.ssl_mode,
            CONNECT_TIMEOUT=str(self.connect_timeout)
        )

    def _execute(self, statement: str, modify: bool=False,
                 values: Iterable=None, **options) -> Optional[DataFrame]:
        conn_str = self._get_connection_string()
        conn = psycopg2.connect(conn_str)
        conn.autocommit = True
        try:
            cur = conn.cursor()
            try:
                if not modify:
                    self.logger.debug(statement)
                    cur.execute(statement)
                    rows = cur.fetchall()
                    column_names = [desc[0] for desc in cur.description]
                    return DataFrame(rows, columns=column_names)
                else:
                    cur.execute(statement, values)
                    conn.commit()
                    return None
            finally:
                conn.close()
        finally:
            for notice in conn.notices:
                self.logger.info("[NOTICE]: " + notice)
            conn.close()

    def select(self, statement: str, **options) -> DataFrame:
        return self._execute(statement, **options)

    def insert(self, upsert: bool=True,
               conflict_target: str='row_hash', **data) -> None:

        columns = data['fields'].keys()
        values = [data['fields'][col]['value'] for col in columns]

        statement = 'INSERT INTO {} ({}) VALUES ({})'.format(
            self.table,
            ', '.join(map(lambda x: '"' + x + '"', columns)),
            ','.join(['%s'] * len(values))
        )

        if upsert:
            validate.not_none(conflict_target)
            statement = '{} ON CONFLICT ({}) DO NOTHING'.format(
                statement,
                conflict_target
            )

        return self._execute(statement=statement, modify=True, values=values)

    def update(self, **options) -> None:
        pass

    def __str__(self) -> str:
        return self._get_connection_string()


class DatabaseFactory:

    __MAPPING: Dict[str, Type[AbstractDatabase]] = {
        "sqlite3": SQLAlchemyDatabase,
        "postgres": PostgresDatabase,
    }

    @staticmethod
    def __find(name: str) -> Type[AbstractDatabase]:
        validate.is_in_dict_keys(name, DatabaseFactory.__MAPPING)
        return DatabaseFactory.__MAPPING[name]

    @classmethod
    def load(cls, config: Dict[str, Any]) -> AbstractDatabase:
        validate.is_in_dict_keys('type', config)
        validate.is_in_dict_keys('connection', config)
        db = DatabaseFactory.__find(config.get('type'))
        return db(config)
