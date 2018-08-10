import argparse
import os
import time
import logging

from typing import Tuple
from watchdog.observers import Observer
from watchdog.events import PatternMatchingEventHandler

from sandbox import catalog_provider
from uploadio.sources.parser import ParserFactory
from uploadio.sources.target import DatabaseTarget
from uploadio.sources import source as src


logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s - %(name)-15s - [%(levelname)-10s] %(message)s"
)
log = logging.getLogger(os.path.basename(__file__))


class PipelineHandler(PatternMatchingEventHandler):
    patterns = ["*.csv"]

    def __init__(self, catalog: str, source_name: str) -> None:
        super().__init__()
        self.catalog = catalog
        self.source_name = source_name

    @staticmethod
    def file(file_name: str) -> str:
        base_path = os.path.abspath(os.path.dirname(__file__))
        return os.path.join(
            base_path,
            f"resources/{file_name}"
        )

    @staticmethod
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
        cat_file = PipelineHandler.json_source(self.catalog)
        log.info("Loading catalog {}".format(cat_file.uri))
        catalog = catalog_provider(cat_file)
        collection = catalog.load(self.source_name)
        log.info("Processing Source: {}".format(event.src_path))
        csv = collection.source.load(uri=event.src_path)
        parser = ParserFactory.load(collection.parser)
        p = parser(source=csv.data, collection=collection)
        log.info("Inserting values into table '{}'".format(
            collection.target_config['connection'].get('table', 'default')
        ))
        DatabaseTarget(config=collection.target_config, parser=p).output()
        log.info("Done...")

    def on_modified(self, event) -> None:
        log.info("on_modified() event occured")
        self.process(event)

    def on_created(self, event) -> None:
        log.info("on_create() event occured")
        self.process(event)

    def on_moved(self, event) -> None:
        log.info("on_moved() event occured")
        self.process(event)


def run(path: str, catalog: str, source_name: str) -> None:
    observer = Observer()
    handler = PipelineHandler(
        catalog=catalog,
        source_name=source_name
    )
    observer.schedule(handler, path)
    observer.start()
    log.info("Watchdog started...")

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        observer.stop()

    observer.join()


def parse_arguments() -> Tuple[str, str, str]:
    parser = argparse.ArgumentParser(description="upload.io runner")
    parser.add_argument('-p',
                        '--path',
                        dest='path',
                        help='path to observe for changes',
                        required=True
                        )
    parser.add_argument('-c',
                        '--catalog',
                        dest='catalog',
                        help='Path to a catalog file',
                        required=True
                        )
    parser.add_argument('-s',
                        '--source',
                        dest='source',
                        help='source name within given catalog',
                        required=True
                        )

    args = parser.parse_args()
    return args.path, args.catalog, args.source


if __name__ == '__main__':
    path, catalog, source_name = parse_arguments()
    run(path, catalog, source_name)
