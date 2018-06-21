import io
import json
import time
from abc import abstractmethod
from typing import Any, Dict, List

import avro.datafile
import avro.io
import avro.schema

from src.p3common.common import validators as validate

from uploadio.common.db import DatabaseFactory
from uploadio.sources.parser import Parser
from uploadio.utils import Loggable, make_md5


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


class DatabaseTarget(Target):

    def __init__(self, config: Dict[str, Any], parser: Parser) -> None:
        validate.is_in_dict_keys('type', config)

        super().__init__(config, parser)
        self.db = DatabaseFactory.load(config)
        self.row_hash = self.config.get('options', {}).get('row_hash', False)
    
    def _output(self, **kwargs) -> None:
        for elem in self.parser.parse(**kwargs):
            if self.row_hash:
                elem = self._apply_row_hash(elem)
                print(elem)
            self.db.insert(**elem)

    def _apply_row_hash(self, elem: Dict[str, Any]) -> Dict[str, Any]:
        fields = elem.get('fields')
        new_elem = {'row_hash': {
            'value': make_md5(str(fields)),
            'datatype': 'string', 
            'mandatory': True
        }}               
        elem['fields'] = {**fields, **new_elem}        
        return elem


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
