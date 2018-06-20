import io
import os

import avro

from uploadio.sources import catalog as cat
from uploadio.sources import source as src
from uploadio.sources.parser import ParserFactory
from uploadio.sources.target import LoggableTarget, AvroTarget, DatabaseTarget
from uploadio.common.db import SQLAlchemyDatabase

def file(file_name: str) -> str:
    base_path = os.path.abspath(os.path.dirname(__file__))
    return os.path.join(
        base_path,
        f"test/resources/{file_name}"
    )


def json_source(path: str) -> src.Source:
    return src.JSONSource(uri=path).load()


def csv_source(path: str, **args) -> src.Source:
    return src.CSVSource(uri=path, **args).load()


def catalog_provider(source: src.Source) -> cat.CatalogProvider:
    return cat.JsonCatalogProvider(source.data)


def read_avro(data) -> None:
    message_buf = io.BytesIO(data)
    reader = avro.datafile.DataFileReader(message_buf, avro.io.DatumReader())
    for thing in reader:
        print(thing)
    reader.close()


def create_table(collection) -> None:
    db = SQLAlchemyDatabase(collection.target_config)
    db.execute("drop table if exists {}".format(collection.name))
    stmt = "create table if not exists {} ({})".format(
        collection.name,
        ', '.join(collection.columns)
    )
    db.execute(stmt)


def select_table(collection) -> None:
    db = SQLAlchemyDatabase(collection.target_config)
    stmt = "select * from {}".format(collection.name)
    return db.select(stmt)


def kontoauszug():
    catalog = catalog_provider(json_source(
        file("sources/catalog_auszug.json")))
    collection = catalog.load('kontoauszug')
    csv = collection.source.load()
    parser = ParserFactory.load(collection.parser)
    # p = parser(source=csv.data, collection=auszug, schema=auszug.schema)
    # schema = json_source(file(auszug.schema)).data
    p = parser(source=csv.data, collection=collection)    
    LoggableTarget(config=collection.target_config, parser=p).output()
    create_table(collection)
    DatabaseTarget(config=collection.target_config, parser=p).output()
    # data = select_table(collection)
    # print(data)


def customer():
    catalog = catalog_provider(json_source(file("sources/catalog1.json")))
    collection = catalog.load('customer')
    csv = collection.source.load()
    parser = ParserFactory.load(collection.parser)
    p = parser(source=csv.data, collection=collection)

    # p = parser(source=csv.data, collection=customer)
    # data = p.parse(namespace=catalog.namespace, version=catalog.version,
    #                source='customer')
    # schema = json_source(file(collection.schema)).data
    target = AvroTarget(config=collection.target_config, parser=p, schema=collection.schema)
    target.output(namespace=catalog.namespace,
                  version=catalog.version,
                  source='customer')
    # read_avro(data)


def http():
    catalog = catalog_provider(json_source(file("sources/http.json")))
    collection = catalog.load('SalesJan2009')
    csv = collection.source.load()
    parser = ParserFactory.load(collection.parser)
    p = parser(source=csv.data, collection=collection)
    target = LoggableTarget(config=collection.target_config, parser=p)
    target.output()


def run():
    kontoauszug()


if __name__ == '__main__':
    run()
