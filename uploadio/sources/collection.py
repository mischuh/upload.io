from typing import Dict, List, Any, Union

from src.p3common.common import validators as validate
from uploadio.sources.source import Source, JSONSource, SourceFactory
from .transformation import Transformation


class Field:
    """
    A Field element in a source.

    In general this is a column in table or a csv file
    or attribute in a json file
    """

    def __init__(self, name: str,
                 data_type: str,
                 default: Any,
                 alias: str=None,
                 transformations: Dict[str, Transformation]=None) -> None:
        self.name = name
        self.data_type = data_type
        self.default = default
        self.alias = alias if alias is not None else name
        self.transformations = transformations if transformations is not None \
            else Dict[str, Transformation]

    def has_alias(self) -> bool:
        return self.alias != self.name

    def __repr__(self) -> str:
        return """
            Field(\
                name='{}', \
                data_type = '{}', \
                default = '{}', \
                alias = '{}', \
                transformations = {}\
            )""".format(
            self.name,
            self.data_type,
            self.default,
            self.alias,
            self.transformations
        )


class SourceDefinition:
    """
    THE description of a :py:class:`Source`
    """

    def __init__(self,
                 name: str,
                 source_config: Dict,
                 target_config: Dict,
                 parser_config: Dict,
                 version: str,
                 fields: Dict[str, Field]) -> None:
        """
        :param name: Name of a source
        :param source:
            Source connection configuration (type, path, options)
        :param target:
            if necessary a target information
        :param version:
            the version of the :py:class:`Catalog`
        :param fields:
             dict of key ``field.name`` and value of :py:class:`Field`
        """
        self.name = name
        self.source_config = source_config
        self.target_config = target_config
        self.parser_config = parser_config
        self.version = version
        self.fields = fields

    def validate(self, src_fields: List) -> bool:
        """Simple approach...
        :param src_fields:
            list of source fields (columns, attributes...)
        :return:
            True if all src_fields are described in the catalog,
            otherwise false
        """
        for name in self.fields.keys():
            if name in src_fields:
                src_fields.remove(name)
            else:
                raise ValueError(
                    "Expected field '{}' is not part of source columns ({})"
                    .format(name, src_fields)
                )

        return len(src_fields) == 0

    def field(self, name: str) -> Union[Field, None]:
        validate.is_in_dict_keys(name, self.fields)
        return self.fields.get(name, None)

    @property
    def source(self) -> Source:
        return SourceFactory.load(self.source_config)

    @property
    def parser(self) -> str:
        validate.is_in_dict_keys('name', self.parser_config)
        return self.parser_config.get('name')

    @property
    def schema(self) -> Dict[str, Any]:
        validate.is_in_dict_keys('options', self.target_config)
        validate.is_in_dict_keys('schema', self.target_config.get('options'))
        src = JSONSource(uri=self.target_config.get('options').get('schema')).load()
        return src.data

    @property
    def columns(self) -> List[str]:
        return [field.alias for field in self.fields.values()]

    def __repr__(self) -> str:
        return "SourceDefinition(name='{}', source_config='{}', " \
            "target_config={}, parser_config='{}', version='{}', fields={})".format(
                self.name, self.source_config, self.target_config,
                self.parser_config, self.version, self.fields
            )
