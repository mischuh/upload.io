import io
import os

import avro
from pandas import DataFrame

from uploadio.common.db import Database, DBConnection
from uploadio.common.translator import Datatype, PostgresTranslator
from uploadio.sources import catalog as cat
from uploadio.sources import source as src
from uploadio.sources.parser import ParserFactory
from uploadio.sources.target import AvroTarget, DatabaseTarget, LoggableTarget


def file(file_name: str) -> str:
    base_path = os.path.abspath(os.path.dirname(__file__))
    return os.path.join(
        base_path,
        f"resources/{file_name}"
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
    db = Database(DBConnection(collection.target_config['connection']))
    db.execute(
        statement="drop table if exists {}".format(collection.name),
        modify=True
    )
    col_types = ['"{}" {}'.format(
            f.alias,
            PostgresTranslator(Datatype(f.data_type)).dialect_datatype()
        ) for f in collection.fields.values()]
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


def select_table(collection) -> DataFrame:    
    db = Database(DBConnection(collection.target_config['connection']))
    stmt = "select * from {}".format(collection.name)
    return db.select(stmt)


def kontoauszug():
    print(file("sandbox/catalog_auszug.json"))
    catalog = catalog_provider(json_source(
        file("sandbox/catalog_auszug.json")))
    collection = catalog.load('kontoauszug')
    source = collection.source.load()
    parser = ParserFactory.load(collection.parser)
    p = parser(
        source=source.data,
        collection=collection,
        options=collection.parser_config.get('options', {})
    )
    LoggableTarget(config=collection.target_config, parser=p).output()
    create_table(collection)
    DatabaseTarget(config=collection.target_config, parser=p).output()
    data = select_table(collection)
    print(data)


def customer():
    catalog = catalog_provider(json_source(file("sandbox/catalog_customer.json")))
    collection = catalog.load('customer')
    csv = collection.source.load()
    parser = ParserFactory.load(collection.parser)
    p = parser(source=csv.data, collection=collection)
    #LoggableTarget(config=collection.target_config, parser=p).output()
    target = AvroTarget(
        config=collection.target_config, parser=p, schema=collection.schema
    )
    target.output(namespace=catalog.namespace,
                  version=catalog.version,
                  source='customer')


def http():
    catalog = catalog_provider(json_source(file("sandbox/http.json")))
    collection = catalog.load('SalesJan2009')
    csv = collection.source.load()
    parser = ParserFactory.load(collection.parser)
    p = parser(
        source=csv.data,
        collection=collection,
        options=collection.parser_config.get('options', {})
    )
    create_table(collection)
    DatabaseTarget(config=collection.target_config, parser=p).output()
    data = select_table(collection)
    print(data.head())


def quotes():
    catalog = catalog_provider(json_source(file(
        "sandbox/catalog_quotes.json")))
    collection = catalog.load('quotes')
    json = collection.source.load()
    parser = ParserFactory.load(collection.parser)
    p = parser(
        source=json.data, 
        collection=collection,
        options=collection.parser_config.get('options', {})
    )
    create_table(collection)
    DatabaseTarget(config=collection.target_config, parser=p).output()
    data = select_table(collection)
    print(data.head())


if __name__ == '__main__':
    customer()
