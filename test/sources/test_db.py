import pytest
from pandas import DataFrame
from schema import And, Optional, Or, Schema, SchemaMissingKeyError, Use

from uploadio.common.db import Database, DBConnection


@pytest.fixture(scope='function')
def target_config():
    yield {
        "connection" : {
            "uri": "sqlite:///pytest.db",
            "table": "pytest",
            "options": {
                'k1': 123
            }
        }
    }

@pytest.fixture(scope='function')
def dbcon_schema():
    yield Schema({
        'uri': Use(str),
        'table': Use(str),
        Optional('options', default={}): Or({}, {Use(str): object})
    })


def test_validate_sqlite3(target_config, dbcon_schema):
    assert dbcon_schema.validate(target_config['connection']) == target_config['connection']


def test_invalid_connection(target_config):  
    config = target_config['connection']
    config['urrri'] = config.pop('uri')
    with pytest.raises(SchemaMissingKeyError):
        DBConnection(config)


@pytest.mark.integration
def test_connection(target_config):
    connection = DBConnection(target_config['connection'])
    conn = connection.connect()
    assert connection.is_connected() == True


@pytest.mark.integration
def test_database(target_config):
    db = Database(connection=DBConnection(target_config['connection']))
    db.execute(
        statement="create table if not exist {} (country text, year int, reports int)".format(
            target_config['connection']['table']
        ),
        modify=True
    )
    data = {'county': ['Cochice', 'Pima', 'Santa Cruz', 'Maricopa', 'Yuma'], 
        'year': [2012, 2012, 2013, 2014, 2014], 
        'reports': [4, 24, 31, 2, 3]}
    df = DataFrame(data)
    db.insert(data=df)
    res = db.select(statement="select * from {}".format(
            target_config['connection']['table']
        ))
    db.execute(
        statement="drop table if exist {}".format(
            target_config['connection']['table']
        ),
        modify=True
    )
    assert len(res) == 5
