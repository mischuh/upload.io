from typing import Any, Dict, List, Union

import attr

from uploadio.sources.source import JSONSource, Source, SourceFactory
from uploadio.sources.transformation import Transformation, TransformationType


@attr.s
class Field:
    """
    A Field element in a source.

    In general this is a column in table or a csv file
    or attribute in a json file
    """
    name: str = attr.ib()
    data_type: str = attr.ib()
    default: Any = attr.ib()
    alias: str = attr.ib(default=name)
    transformations: Dict[str, Transformation] = attr.ib(
        default=Dict[str, Transformation]
    )

    def rules(self) -> Union[Dict[str, Transformation], None]:
        return dict(filter(lambda x: x[1].type == TransformationType.RULE, self.transformations.items()))

    def filters(self) -> Union[Dict[str, Transformation], None]:
        return dict(filter(lambda x: x[1].type == TransformationType.FILTER, self.transformations.items()))

    def has_alias(self) -> bool:
        return self.alias is not None and self.alias != self.name


@attr.s
class SourceDefinition:
    """
    THE description of a :py:class:`Source`
    :param name: Name of a source
    :param source_config:
        Source connection configuration (type, path, options)
    :param target_config:
    if necessary a target information
    :param version:
    the version of the :py:class:`Catalog`
    :param fields:
    dict of key ``field.name`` and value of :py:class:`Field`
    """
    name: str = attr.ib()
    source_config: Dict[str, Any] = attr.ib()
    target_config: Dict[str, Any] = attr.ib()
    parser_config: Dict[str, Any] = attr.ib()
    version: str = attr.ib()
    fields: Dict[str, Field] = attr.ib()

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
        # validate.is_in_dict_keys(name, self.fields)
        return self.fields.get(name, None)

    @property
    def source(self) -> Source:
        return SourceFactory.load(self.source_config)

    @property
    def parser(self) -> str:
        # validate.is_in_dict_keys('name', self.parser_config)
        return self.parser_config.get('name')

    @property
    def schema(self) -> Dict[str, Any]:
        # validate.is_in_dict_keys('connection', self.target_config)
        # validate.is_in_dict_keys(
        #     'schema', self.target_config.get('connection')
        # )
        src = JSONSource(
            uri=self.target_config.get('connection').get('schema')
        ).load()
        return src.data

    @property
    def columns(self) -> List[str]:
        return [field.alias for field in self.fields.values()]
