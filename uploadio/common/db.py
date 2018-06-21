import sqlite3
from abc import abstractmethod
from collections import OrderedDict
from typing import Any, Dict, List, Optional, Type, Union

from pandas import DataFrame

from src.p3common.common import validators as validate

from .collections import get_value_from_dictionary


class ConfigurationError(Exception):
    pass


# class ConnectionInfo:

#     @classmethod
#     def from_file(cls, connection_file: str) -> Any:
#         import json
#         with open(connection_file) as cur:
#             conn = json.load(cur)
#             return cls().parse(conn)

#     @classmethod
#     def from_dict(cls, kv: Dict[str, Any]) -> Any:
#         return cls().parse(kv)

#     @abstractmethod
#     def parse(self, connection_json: Dict[str, Any]) -> Any:
#         """

#         :param connection_json:
#         :return:
#         """
#         raise NotImplementedError(
#             "You have to implment this method in a subclass, dude!")

#     @staticmethod
#     def _get_value_from_dict_or_die(d: Dict[str, Any], *keys) -> Any:
#         """keys: list of possible key candidates"""
#         value = ConnectionInfo._get_value_from_dict_or_default(
#             d, None, *keys)

#         if value is None or not value:
#             raise ConfigurationError("Failed to initialize database connection info. '{}' are not defined"
#                                      .format(', '.join(keys)))

#         return value

#     @staticmethod
#     def _get_value_from_dict_or_default(d: Dict[str, Any], default: Any, *keys) -> Any:
#         """keys: list of possible key candidates"""
#         # Probe our keys against the dictionary and memorize the result
#         values = [get_value_from_dictionary(d, key, None) for key in keys]
#         # Find next not None
#         value = next((item for item in values if item is not None), 'not none')

#         return value if value != 'not none' else default


# class DBConnectionInfo(ConnectionInfo):

#     def parse(self, connection_json: Dict[str, Any]) -> Any:
    
#         self.endpoint = GenericApiClient._get_value_from_dict_or_die(connection_json, 'specific.endpoint', 'endpoint')
#         self.user = GenericApiClient._get_value_from_dict_or_die(connection_json, 'specific.user', 'user')
#         self.pwd = GenericApiClient._get_value_from_dict_or_die(connection_json, 'specific.pwd', 'pwd')
#         self.headers = GenericApiClient._get_value_from_dict_or_die(connection_json, 'specific.headers', 'headers')

#         is_encrypted = GenericApiClient._get_value_from_dict_or_default(
#             connection_json, None, 'specific.is_encrpted', 'is_encrpted'
#         )
#         if is_encrypted is None:
#             # This one does probe for the non-typo is_encrypted
#             is_encrypted = GenericApiClient._get_value_from_dict_or_default(
#                 connection_json, False, 'specific.is_encrypted', 'is_encrypted'
#             )

#         if is_encrypted:
#             self.pwd = CryptFactory.get(key=ZuoraApiClient.KEY).decrypt(self.pwd)

#         self.auth = (self.user, self.pwd)
#         self.is_parsed = True

#         return self


class AbstractDatabase:

    def __init__(self, connection: Dict[str, Any]) -> None:                
        self.connection = connection

    @abstractmethod
    def select(self, statement: str, **options) -> DataFrame:
        raise NotImplementedError()

    @abstractmethod
    def insert(self, *args, **options) -> None:
        raise NotImplementedError()

    @abstractmethod
    def update(self, **options) -> None:
        raise NotImplementedError()

    @abstractmethod
    def upsert(self, **options) -> None:
        raise NotImplementedError()


class SQLAlchemyDatabase(AbstractDatabase):

    def __init__(self, connection: Dict[str, Any]) -> None:
        super().__init__(connection)
        self.database = "{}/{}.db".format(
            self.connection['connection']['uri'], 
            self.connection['connection']['database']
        )
        # TODO: Validate config https://pypi.org/project/schema/
        self.table = self.connection['connection']['table']
        self.options = self.connection.get('options', {})

    def get_sqlalchemy_conn_str(self) -> str:
        return 'sqlite:///{}'.format(self.database)

    def execute(self, statement: str) -> None:
        conn = sqlite3.connect(self.database)
        cur = conn.cursor()
        try:
            print(statement)
            cur.execute(statement)
        finally:
            cur.close()

    def select(self, statement: str, **options) -> DataFrame:
        conn = sqlite3.connect(self.database)
        cur = conn.cursor()
        try:            
            cur.execute(statement)
            rows = cur.fetchall()            
            column_names = [desc[0] for desc in cur.description]
            return DataFrame(rows, columns=column_names)
        finally:
            cur.close()

    def insert(self, **data) -> None:
        
        columns =  data['fields'].keys()
        values = [data['fields'][col]['value'] for col in columns]   

        exec_text = 'INSERT INTO {} ({}) values({})'.format(
            self.table, 
            ', '.join(map(lambda x: "'" + x + "'", columns)), 
            ', '.join('?' * len(values))
        )
        conn = sqlite3.connect(self.database)
        cur = conn.cursor()
        cur.execute(exec_text, values)        
        conn.commit()

    def update(self, **options) -> None:
        pass

    def upsert(self, **options) -> None:
        pass

    def __str__(self) -> str:
        return self.get_sqlalchemy_conn_str()

    def __repr__(self) -> str:
        return "{}(uri='{}', database='{}')".format(
            self.__class__.__name__, 
            repr(self.connection['uri']),
            repr(self.connection['options']['database'])
    )


class DatabaseFactory:

    __MAPPING: Dict[str, Type[AbstractDatabase]] = {
        "sqlite3": SQLAlchemyDatabase,
        # "postgres": JSONSource,        
    }

    @staticmethod
    def __find(name: str) -> Type[AbstractDatabase]:
        validate.is_in_dict_keys(name, DatabaseFactory.__MAPPING)
        return DatabaseFactory.__MAPPING[name]

    @classmethod
    def load(cls, config: Dict[str, Any]) -> AbstractDatabase:
        validate.is_in_dict_keys('type', config)
        validate.is_in_dict_keys('connection', config)
        validate.is_in_dict_keys('uri', config.get('connection'))
        db = DatabaseFactory.__find(config.get('type'))
        return db(config)
