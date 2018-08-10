import argparse
import os
import time
from typing import Tuple, Dict, Any

from watchdog.events import PatternMatchingEventHandler
from watchdog.observers import Observer

from sources.collection import SourceDefinition
from uploadio.common.db import DatabaseFactory
from uploadio.common.translator import Datatype, PostgresTranslator
from uploadio.sources import source as src
from uploadio.sources.catalog import JsonCatalogProvider
from uploadio.sources.parser import ParserFactory
from uploadio.sources.target import DatabaseTarget
from uploadio.utils import Loggable

log = Loggable().logger


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

    @staticmethod
    def create_table(collection: SourceDefinition) -> None:
        """
        Auxiliary method to create database and table if you start
        on a vanilla system
        :param collection: target config from catalog
        """

        def table_available(collection: SourceDefinition) -> bool:
            db = DatabaseFactory.load(collection.target_config)
            stmt = "select * from {} limit 1".format(collection.name)
            return db.select(stmt) is not None

        if not table_available(collection):
            log.info("No database table available. Going to create "
                     "everything...")
            db = DatabaseFactory.load(collection.target_config)
            db.execute(
                statement="drop table if exists {}".format(collection.name),
                modify=True
            )
            col_types = []
            for f in collection.fields.values():
                col_types.append('"{}" {}'.format(
                    f.alias,
                    PostgresTranslator(
                        Datatype(f.data_type)
                    ).dialect_datatype()
                ))
            cols = ', '.join(col_types)
            target_options = collection.target_config.get('options', {})
            row_hash = target_options.get('row_hash', False)
            if row_hash:
                cols += ', row_hash text PRIMARY KEY'
            stmt = "create table if not exists {} ({} )".format(
                collection.target_config['connection'].get('table', 'default'),
                cols
            )
            db.execute(statement=stmt, modify=True)
        else:
            log.info("Database table already exists. Nothing to do here...")

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
        catalog = JsonCatalogProvider(cat_file.data)
        collection = catalog.load(self.source_name)
        PipelineHandler.create_table(collection)
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
