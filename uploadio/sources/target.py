from uploadio.sources.parser import Parser
from typing import Dict, Any
from abc import abstractmethod
from uploadio.utils import Loggable


class Target(Loggable):

    def __init__(self, config: Dict[str, Any], parser: Parser) -> None:
        self.config = config
        self.parser = parser

    def output(self, *args, **kwargs) -> None:
        self._output(*args, **kwargs)

    @abstractmethod
    def _output(self, *args, **kwargs) -> None:
        raise NotImplementedError()


class LoggableTarget(Target):

    def _output(self, *args, **kwargs) -> None:

        for elem in self.parser.parse(**kwargs):
            print(elem)
            self.logger.info(elem)
