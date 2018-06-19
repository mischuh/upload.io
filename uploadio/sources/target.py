import io
import json
import time
from abc import abstractmethod
from typing import Any, Dict

import avro.datafile
import avro.io
import avro.schema
from confluent_kafka import Producer


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


class AvroTarget(Target):

    def __init__(self, config: Dict[str, Any], parser: Parser,
                 schema: Dict[str, Any]) -> None:
        super().__init__(config, parser)
        self.schema = avro.schema.Parse(json.dumps(schema))
        self.producer = Producer({
            'bootstrap.servers': config['options']['servers']
        })        

    def __delivery_report(self, err, msg):
        """
        Called once for each message produced to indicate delivery result.
        Triggered by poll() or flush().
        """
        if err is not None:
            print(f"Message delivery failed: {err}")
        else:
            print(f"Message delivered to topic='{msg.topic()}', partition=[{msg.partition()}]")

    def _output(self,
                namespace: str = '',
                version: str = '',
                source: str = '',
                **kwargs) -> None:

        writer = avro.io.DatumWriter(self.schema)
        buf = io.BytesIO()
        # writer = avro.datafile.DataFileWriter(
        #     buf, avro.io.DatumWriter(), self.schema)
        encoder = avro.io.BinaryEncoder(buf)
        ts = int(time.time())
        for elem in self.parser.parse(**kwargs):
            # Trigger any available delivery report callbacks from
            # previous produce() calls
            self.producer.poll(0)
            writer.write(
                {
                    "event_id": make_md5(str(elem.get('fields'))),
                    "event_date": ts,
                    "namespace": namespace,
                    "version": version,
                    "source": source,
                    "columns": list(elem['fields'].keys()),
                    "data": elem
                },
                encoder
            )
            data = buf.getvalue()
            # Asynchronously produce a message, the delivery report callback
            # will be triggered from poll() above, or flush() below, w
            # hen the message has been successfully delivered or
            # failed permanently.
            self.producer.produce(
                topic=self.config['options'].get('topic', '_default'),
                value=data,
                callback=self.__delivery_report
            )
        self.producer.flush()
        buf.seek(0)        
