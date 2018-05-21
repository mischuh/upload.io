"""Test Source Provider"""
import os

import pandas as pd
import pytest

from uploadio.sources import source as src


@pytest.yield_fixture(scope="function")
def csv_path() -> str:
    base_path = os.path.abspath(os.path.dirname(__file__))
    yield os.path.join(base_path, "../resources/sources/test1.csv")


@pytest.yield_fixture(scope="function")
def json_path() -> str:
    base_path = os.path.abspath(os.path.dirname(__file__))
    yield os.path.join(base_path, "../resources/sources/test1.json")


def test_csv_source_provider(csv_path: str) -> None:
    csv_source = src.CSVSource(uri=csv_path).load(sep=',')
    assert isinstance(csv_source, src.Source)
    assert isinstance(csv_source.data, pd.DataFrame)
    assert csv_source.data.size > 0
    assert csv_source.data.columns.all() in [
        'firstname', 'lastname', 'street', 'city', 'zipcode']


def test_json_source_provider(json_path: str) -> None:
    json_source = src.JSONSource(uri=json_path).load(df=True)
    assert isinstance(json_source, src.Source)
    assert isinstance(json_source.data, pd.DataFrame)
    assert json_source.data.size > 0
    assert json_source.data['city'][0] == 'Hamburg'


def test_source_factory() -> None:

    config: Dict[str, str] = {
        'type': 'csv',
        'uri': './test/resources/sources/test1.csv',
        'options': {
            'encoding': 'utf-8',
            'delimiter': ','
        }
    }
    source = src.SourceFactory.load(config)
    assert isinstance(source, src.CSVSource)
    assert isinstance(source, src.Source)
