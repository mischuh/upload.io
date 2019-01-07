from abc import abstractmethod
from collections import deque
from typing import Any, Dict, List, Type

import pandas as pd

from src.p3common.common import validators as validate
from src.p3common.common.validators.utils import ValidationException
from uploadio.sources.collection import Field, SourceDefinition
from uploadio.utils import Loggable, make_md5


class Parser(Loggable):
    """
    Abstract Parser class

    A parser gets a ``SourceDefinition`` which holds all the information about
    a given source (datatypes, transformations...).

    The base idea is, that the parser iterates over the source and does
    something with it. E.g. convert it into AvroEvents or in a very simple way
    only print the information to stdout....
    """

    def __init__(
            self,
            source: pd.DataFrame,
            collection: SourceDefinition,
            **options) -> None:
        """
        :param source: The source represented as a pandas.DataFrame
        :param collection: The information about the source
        """
        self.source = source
        self.collection = collection
        self.options = options or {}

    @abstractmethod
    def parse(self, *args, **kwargs) -> Any:
        """
        Do something with the source data
        """
        raise NotImplementedError()


class SimpleParser(Parser):

    def parse(self, *args, **kwargs) -> Dict:
        """
        Iterates over the given source and prints the data to stdout
        :param args:
        :param kwargs:
        :return:
        """
        if not self.collection.validate(list(self.source.columns)):
            raise ValueError(
                f"Number of source columns ({len(self.source.columns)}) "
                f"vs. target ({len(self.collection.fields)}) do not match!"
            )

        for rix, row in self.source.iterrows():
            for column, value in row.iteritems():
                try:
                    field = self.collection.field(column)
                    for _, trans in sorted(field.transformations.items()):
                        value = trans.transform(value)
                    column = field.alias if field.has_alias() else column
                except ValidationException as why:
                    print(why)

                yield dict(index=str(rix), column=column, value=value)


class JSONEventParser(Parser):

    def parse(self, *args, **kwargs) -> Dict[str, Dict[str, Dict[str, Any]]]:
        """
        Does the actual work...
        """
        self.collection.validate(list(self.source.columns))

        for rix, row in self.source.iterrows():
            fields = dict()
            for column, value in row.iteritems():
                try:
                    field = self.collection.field(column)
                    datatype = field.data_type
                    mandatory = field.data_type is not None
                    for _, trans in sorted(field.transformations.items()):
                        value = trans.transform(value)
                    column = field.alias if field.has_alias() else column
                except ValidationException as why:
                    print(why)
                fields[column] = dict(
                    value=str(value), datatype=datatype, mandatory=mandatory
                )

            yield dict(index=str(rix), fields=fields)


class DBOutputParser(Parser):
    """
    DBOutputParser converts a rows into chunks which are
    optimized to bulk insert. Fromat goes like this:
    {
        'column_name1': [list of tuples of values over chunked rows],
        'column_name2': [list of tuples of values over chunked rows],
        …
    }
    """
    def parse(self, *args, **kwargs) -> List:
        """
        Iterates over the given source and prints the data to stdout
        :param args:
        :param kwargs:
        :return:
        """

        if not self.collection.validate(list(self.source.columns)):
            raise ValueError(
                f"Number of source columns ({len(self.source.columns)}) "
                f"vs. target ({len(self.collection.fields)}) do not match!"
            )

        result = self.source.copy()
        for column in result.columns:
            field = self.collection.field(column)
            for _, trans in sorted(field.transformations.items()):
                result[column] = result[column].apply(
                    lambda x: trans.transform(x)
                )
            if field.has_alias():
                result.rename(columns={column: field.alias}, inplace=True)
    
        if self.options.get('options', {}).get('row_hash', False):
            result['row_hash'] = pd.Series(
                (make_md5(str(row)) for i, row in result.iterrows())
            )
            result.set_index('row_hash', inplace=True)
        
        return result


class ParserFactory:
    """
    Knows which concrete Parser belongs to the name in the data catalog
    "target": {
                "parser": "AvroEvent",
                "connection": {
                    "uri": "",
                    "schema": "schema_new.avsc"
                }
            },
    """

    __MAPPING: Dict[str, Type[Parser]] = {
        "StdOut": SimpleParser,
        "JSONEvent": JSONEventParser,
        "DBOut": DBOutputParser
    }

    @staticmethod
    def load(name: str) -> Type[Parser]:
        validate.is_in_dict_keys(name, ParserFactory.__MAPPING)
        return ParserFactory.__MAPPING[name]
