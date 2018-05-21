from abc import abstractmethod
from typing import Dict, Type, Any

import pandas as pd

from src.p3common.common import validators as validate
from src.p3common.common.validators.utils import ValidationException
from uploadio.sources.collection import SourceDefinition
from uploadio.utils import Loggable


class Parser(Loggable):
    """
    Abstract Parser class

    A parser gets a ``SourceDefinition`` which holds all the information about
    a given source (datatypes, transformations...).

    The base idea is, that the parser iterates over the source and does
    something with it. E.g. convert it into AvroEvents or in a very simple way
    only print the information to stdout....
    """

    def __init__(self, source: pd.DataFrame, collection: SourceDefinition) -> None:
        """
        :param source: The source represented as a pandas.DataFrame
        :param collection: The information about the source
        """
        self.source = source
        self.collection = collection

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
            raise ValueError(f"Number of source columns ({len(self.source.columns)}) "
                             f"vs. target ({len(self.collection.fields)}) do not match!")

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

    def parse(self):
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
                    value=str(value), datatype=datatype, mandatory=mandatory)

            yield dict(index=str(rix), fields=fields)


class ParserFactory:
    """
    Knows which conrete Parser belongs to the name in the data catalog
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
        "JSONEvent": JSONEventParser
    }

    @staticmethod
    def load(name: str) -> Type[Parser]:
        validate.is_in_dict_keys(name, ParserFactory.__MAPPING)
        return ParserFactory.__MAPPING[name]
