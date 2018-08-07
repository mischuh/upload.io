import os
import time
import sys
import logging
from watchdog.observers import Observer
from watchdog.events import PatternMatchingEventHandler

from sandbox import catalog_provider
from uploadio.sources.parser import ParserFactory
from uploadio.sources.target import DatabaseTarget
from uploadio.sources import source as src

log = logging.getLogger(__name__)


class MyHandler(PatternMatchingEventHandler):
    patterns = ["*.csv"]

    def __init__(self, catalog: str, source_name: str) -> None:
        super().__init__()
        self.catalog = catalog
        self.source_name = source_name

    def json_source(path: str) -> src.Source:
        return src.JSONSource(uri=path).load()

    def process(self, event) -> None:
        """
        event.event_type
            'modified' | 'created' | 'moved' | 'deleted'
        event.is_directory
            True | False
        event.src_path
            path/to/observed/file
        """
        # the file will be processed there
        print("Loading catalog '{}".format(self.catalog))
        catalog = catalog_provider(self.json_source(self.catalog))
        collection = catalog.load(self.source_name)
        print("Processing Source: {}".format(event.src_path))
        csv = collection.source.load(uri=event.src_path)
        parser = ParserFactory.load(collection.parser)
        p = parser(source=csv.data, collection=collection)
        print("Inserting values into table '{}'".format(
            collection.target_config['connection'].get('table', 'default')
        ))
        DatabaseTarget(config=collection.target_config, parser=p).output()
        print("Done...")

    def on_modified(self, event) -> None:
        print("on_modified() event occured")
        self.process(event)

    def on_created(self, event) -> None:
        print("on_create() event occured")
        self.process(event)

    def on_moved(self, event) -> None:
        print("on_moved() event occured")
        self.process(event)


if __name__ == '__main__':
    args = sys.argv[1:]
    observer = Observer()
    handler = MyHandler(
        catalog='/Users/mschuh/src/private/upload.io/resources/catalog_auszug.json',
        source_name='kontoauszug'
    )
    observer.schedule(handler, path=args[0] if args else '.')
    observer.start()
    print("Watchdog started...")

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        observer.stop()

    observer.join()
