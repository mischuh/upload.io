from abc import abstractmethod
from typing import Any, Dict, Iterable, Optional

import attr
import schema
from pandas import DataFrame
from sqlalchemy import create_engine
from sqlalchemy.engine.base import Connection, Engine

from uploadio.utils import LogMixin


@attr.s
class DBConnection(LogMixin):
    """
    Wraps the database (sqlalchemy) connection

    For further :py:class:`sqlalchemy.engine.Engine` information,
    especially for the `config['options']` part,  see here:
    https://docs.sqlalchemy.org/en/latest/core/engines.html

    For further :py:class:`sqlalchemy.engine.Connection` information see here.
    Provides high-level functionality for a wrapped DB-API connection.
    https://docs.sqlalchemy.org/en/latest/core/connections.html
    """

    connection_schema = schema.Schema({
        'uri': schema.Use(str),
        'table': schema.Use(str),
        schema.Optional('options', default={}): schema.Or(
            {}, {schema.Use(str): object}
        )
    })

    def validate(self, instance, attribute) -> bool:
        """ validators runs after the instance is initialized
        this is why `self` works here """
        return self.connection_schema.is_valid(attribute)

    config: Dict[str, Any] = attr.ib(validator=validate)
    connection: Connection = attr.ib(init=False, repr=False)
    engine: Engine = attr.ib(init=False, repr=False)

    def __attrs_post_init__(self) -> None:
        self.config = self.connection_schema.validate(self.config)
        self.engine = create_engine(self.config['uri'])

    def connect(self) -> Connection:
        self.logger.debug(
            "Establishing DB connection with {}".format(self.__repr__())
        )
        self.connection = self.engine.connect()
        return self.connection

    def __exit__(self):
        self.close()

    def close(self) -> None:
        self.logger.debug("Closing DB connection...")
        self.connection.close()

    def is_connected(self) -> bool:
        return not self.connection.closed


@attr.s
class Database(LogMixin):
    """
    :py:class:`DBConnection`
    """

    connection: DBConnection = attr.ib()

    def execute(self, statement: str, modify: bool = False,
                data: Iterable = None) -> Optional[DataFrame]:
        """
        Executes a generic SQL statement on a `DBConnection`

        :param statement: valid (for specified engine) SQL statement
        :param modify: True if SQL modifies table entries (e.g. insert, update)
        :param data: specify a fixed VALUES clause for an INSERT statement,
                    or the SET clause for an UPDATE
        """
        data = [] if not data else data
        conn = self.connection.connect()
        try:
            self.logger.info(statement)
            if not modify:
                result = conn.execute(statement)
                rows = result.fetchall()
                column_names = list(result.keys())
                return DataFrame(rows, columns=column_names)
            else:
                conn.execute(statement, data)
                return None
        except Exception:
            import traceback
            self.logger.error(
                "Error when executing database transaction: {}".format(
                    traceback.format_exc()
                )
            )
        finally:
            self.connection.close()

        return None

    def select(self, statement: str, **options) -> DataFrame:
        return self.execute(statement, **options)

    def insert(self, data: DataFrame, chunksize: int = 100) -> None:
        self.connection.connect()
        data.to_sql(
            self.connection.config['table'],
            con=self.connection.engine,
            if_exists='replace',
            chunksize=chunksize,
            schema=self.connection.config['options'].get('schema', None)
        )

    @abstractmethod
    def update(self, **options) -> None:
        raise NotImplementedError()
