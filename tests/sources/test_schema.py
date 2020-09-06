import os
from typing import Any, Dict
from uploadio.sources import source as src
import pytest
from schema import Schema

from tests import schema


@pytest.yield_fixture(scope="function")
def json_path() -> Dict[str, Any]:
    base_path = os.path.abspath(os.path.dirname(__file__))
    yield os.path.join(base_path, "../resources/test_schema.json")


def test_schema() -> None:
    assert type(schema) == Schema


def test_validate_schema(json_path) -> None:
    json_source = src.JSONSource(uri=json_path).load(df=False)
    schema.validate(json_source.data)
