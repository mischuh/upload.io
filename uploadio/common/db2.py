import sqlite3
from abc import abstractmethod
from collections import OrderedDict

from pandas import DataFrame

from .collections import get_value_from_dictionary
from typing import Dict, Any, Optional


class ConfigurationError(Exception):
    pass


class ConnectionInfo:

    @classmethod
    def from_file(cls, connection_file: str) -> Any:
        import json
        with open(connection_file) as cur:
            conn = json.load(cur)
            return cls().parse(conn)

    @classmethod
    def from_dict(cls, kv: Dict[str, Any]) -> Any:
        return cls().parse(kv)

    @abstractmethod
    def parse(self, connection_json: Dict[str, Any]) -> Any:
        """

        :param connection_json:
        :return:
        """
        raise NotImplementedError(
            "You have to implment this method in a subclass, dude!")

    @staticmethod
    def _get_value_from_dict_or_die(d: Dict[str, Any], *keys) -> Any:
        """keys: list of possible key candidates"""
        value = ConnectionInfo._get_value_from_dict_or_default(
            d, None, *keys)

        if value is None or not value:
            raise ConfigurationError("Failed to initialize database connection info. '{}' are not defined"
                                     .format(', '.join(keys)))

        return value

    @staticmethod
    def _get_value_from_dict_or_default(d: Dict[str, Any], default: Any, *keys) -> Any:
        """keys: list of possible key candidates"""
        # Probe our keys against the dictionary and memorize the result
        values = [get_value_from_dictionary(d, key, None) for key in keys]
        # Find next not None
        value = next((item for item in values if item is not None), 'not none')

        return value if value != 'not none' else default


class AbstractDatabase(ConnectionInfo):

    def __init__(self, db_path: str) -> None:
        super().__init__()
        self.db_path = db_path

    @abstractmethod
    def parse(self, connection_json) -> ConnectionInfo:
        raise NotImplementedError()

    @abstractmethod
    def select(self, statement: str, **options) -> DataFrame:
        raise NotImplementedError()

    @abstractmethod
    def insert(self, **options) -> None:
        raise NotImplementedError()

    @abstractmethod
    def update(self, **options) -> None:
        raise NotImplementedError()

    @abstractmethod
    def upsert(self, **options) -> None:
        raise NotImplementedError()


class SQLAlchemyDatabase(AbstractDatabase):

    def __init__(self, path: str=None) -> None:
        super().__init__(db_path=path)

    def get_sqlalchemy_conn_str(self) -> str:
        return 'sqlite:///{}'.format(self.db_path)

    def parse(self, connection_json) -> ConnectionInfo:
        self.db_path = ConnectionInfo._get_value_from_dict_or_die(
            connection_json, 'db_path'
        )
        return self

    def select(self, statement: str, **options) -> DataFrame:
        conn = sqlite3.connect(self.db_path)
        cur = conn.cursor()
        try:
            cur.execute(statement)
            rows = cur.fetchall()
            column_names = [desc[0] for desc in cur.description]
            return DataFrame(rows, columns=column_names)
        finally:
            cur.close()

    def insert(self, **options) -> None:
        conn = sqlite3.connect(self.db_path)
        cur = conn.cursor()
        for item in range(1, 2):
            cur.execute('insert into tablename values (?,?,?)', item)

    def update(self, **options) -> None:
        pass

    def upsert(self, **options) -> None:
        pass

    def __str__(self) -> str:
        return self.get_sqlalchemy_conn_str()

    def __repr__(self) -> str:
        return "{}(db_path={})".format(self.__class__.__name__, repr(self.db_path))
