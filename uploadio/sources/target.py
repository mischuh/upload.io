import io
import json
import time
from abc import abstractmethod
from typing import Any, Dict, Iterable

import attr
import avro.datafile
import avro.io
import avro.schema

from uploadio.common.db import Database, DBConnection
from uploadio.sources.parser import Parser
from uploadio.utils import Loggable, make_md5


@attr.s
class Target(Loggable):

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
            print(elem)
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
       

class AvroTarget(Target):

    def __init__(self, config: Dict[str, Any], parser: Parser,
                 schema: Dict[str, Any]) -> None:
        super().__init__(config, parser)
        self.schema = avro.schema.Parse(json.dumps(schema))

    def _output(self,
                namespace: str = '',
                version: str = '',
                source: str = '',
                **kwargs) -> None:

        buf = io.BytesIO()
        writer = avro.datafile.DataFileWriter(
            buf, avro.io.DatumWriter(), self.schema)
        ts = int(time.time())
        for elem in self.parser.parse(**kwargs):
            writer.append({
                "event_id": make_md5(str(elem.get('fields'))),
                "event_date": ts,
                "namespace": namespace,
                "version": version,
                "source": source,
                "columns": list(elem['fields'].keys()),
                "data": elem
            })
        writer.flush()
        buf.seek(0)
        print(buf.read())
