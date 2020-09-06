from abc import abstractmethod
from typing import Any, Dict

import attr

from uploadio.common.db import Database, DBConnection
from uploadio.sources.parser import Parser
from uploadio.utils import LogMixin


@attr.s
class Target(LogMixin):
    config: Dict[str, Any] = attr.ib()
    parser: Parser = attr.ib()

    def output(self, *args, **kwargs) -> None:
        self._output(*args, **kwargs)

    @abstractmethod
    def _output(self, *args, **kwargs) -> None:
        raise NotImplementedError()


class LoggableTarget(Target):

    def _output(self, *args, **kwargs) -> None:
        for elem in self.parser.parse(**kwargs):
            self.logger.info(elem)


class DatabaseTarget(Target):

    def __init__(self, config: Dict[str, Any], parser: Parser) -> None:
        super().__init__(config, parser)
        self.db = Database(connection=DBConnection(self.config['connection']))

    def _output(self, **kwargs) -> None:
        self.db.insert(
            data=self.parser.parse(**kwargs),
            chunksize=self.config.get('options', {}).get('chunksize', None)
        )


class MessageQueueTarget(Target):

    def _output(self, *args, **kwargs) -> None:
        pass
