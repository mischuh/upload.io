import pandas as pd
import pytest

from uploadio.sources.transformation import (Task, Transformation,
                                             TransformationFactory)


@pytest.fixture(scope='function')
def data() -> pd.DataFrame:
    data = {
        'name': ['Jason', 'Molly', 'Tina', 'Jake', 'Amy'], 
        'year': [2012, 2012, 2013, 2014, 2014], 
        'reports': [4, 24, 31, 2, 3],
        'coverage': [25, 94, 57, 62, 70]
    }
    yield pd.DataFrame(
        data,
        index = ['Cochice', 'Pima', 'Santa Cruz', 'Maricopa', 'Yuma']
    )

@pytest.fixture(scope='function')
def replace_rule() -> Transformation:
    clz = TransformationFactory.load('replace')
    rt = Task(
        name='replace',
        operator={
            "new": "***",
            "old": "fuc"
        }
    )
    return clz(task=rt, order=0)

@pytest.fixture(scope='function')
def regexreplace_rule() -> Transformation:
    clz = TransformationFactory.load('regexreplace')
    rt = Task(
        name='regexreplace',
        operator={
            "old": "\*",
            "new": "+",
        }
    )
    return clz(task=rt, order=0)

@pytest.fixture(scope='function')
def uppercase_rule() -> Transformation:
    clz = TransformationFactory.load('uppercase')
    rt = Task(
        name='uppercase',
        operator=None
    )
    return clz(task=rt, order=0)

@pytest.fixture(scope='function')
def lambda_rule() -> Transformation:
    clz = TransformationFactory.load('lambda')
    rt = Task(
        name='lambda',
        operator="lambda x: x[::-1]"
    )
    return clz(task=rt, order=0)


@pytest.fixture(scope='function')
def date_format_rule() -> Transformation:
    clz = TransformationFactory.load('date_format')
    rt = Task(
        name='date_format',
        operator={
            "from": "%m/%d/%Y",
            "to": "%Y-%m-%d"
        }
    )
    return clz(task=rt, order=0)


@pytest.fixture(scope='function')
def num_cmp_filter() -> Transformation:
    clz = TransformationFactory.load('comparison')
    rt = Task(
        name='comparison',
        operator={
            "expression": "le",
            "other": 10
        }
    )
    return clz(task=rt, order=0)


def test_numeric_comparison(num_cmp_filter: Transformation):
    """
    given value (50) is not less or equals other value (10).
    with the logic in mind that we want to filter values this the
    correct result
    """
    assert num_cmp_filter.transform(value=50) == True


def test_replace_rule(replace_rule: Transformation):
    assert replace_rule.transform(value='fuchs') == '***hs'


def test_regexreplace_rule(
    replace_rule: Transformation, 
    regexreplace_rule: Transformation):
    assert regexreplace_rule.transform(
            value=replace_rule.transform(value='fuchs')
        ) == '+++hs'


def test_uppercase_rule(uppercase_rule: Transformation):
    assert uppercase_rule.transform(value='fuchs') == 'FUCHS'


def test_lambda_rule(lambda_rule: Transformation):
    assert lambda_rule.transform(value='fuchs') == 'shcuf'


def test_date_format_rule(date_format_rule: Transformation):
    assert date_format_rule.transform(value='12/24/2018') == '2018-12-24'

def test_filter_on_df(data: pd.DataFrame, num_cmp_filter: Transformation):
    pass
