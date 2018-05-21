"""Test Source Provider"""
import os
import pytest
import pandas as pd
from typing import Dict, Any
from uploadio.sources import catalog as cat
from uploadio.sources import source as src


@pytest.yield_fixture(scope="function")
def catalog() -> str:
    base_path = os.path.abspath(os.path.dirname(__file__))
    yield os.path.join(base_path, "../resources/sources/catalog1.json")


@pytest.yield_fixture(scope="function")
def source(catalog: str) -> src.Source:
    yield src.JSONSource(uri=catalog).load()


def test_catalog(source: src.Source) -> None:
    catalog = cat.JsonCatalogProvider(source.data)
    assert {'customer'} & set(catalog.list_sources())


def test_has_source(source: src.Source) -> None:
    catalog = cat.JsonCatalogProvider(source.data)
    source_name = catalog.list_sources()[0]
    assert catalog.has_source(source_name) is True
