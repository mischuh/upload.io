import abc
from typing import Any, Dict, List

from src.p3common.common import validators as validate
from uploadio.sources.collection import Field, SourceDefinition
from uploadio.sources.transformation import (FilterTask, FilterTransformation,
                                             RuleTask, Transformation,
                                             TransformationFactory,
                                             TransformationType)


class ConfigurationError(Exception):
    pass


class CatalogProvider:
    """
    A Catalog holds the schema and transformation rules for a customers source.

    In a Catalog you will find information about, which field you
    expect in a source in which  are being used
    to _understand_ the customer data
    """
    @abc.abstractmethod
    def load(self, source: str) -> SourceDefinition:
        """
        Loads a source definition from a catalog
        """
        raise NotImplementedError("You have to implement this.")

    @abc.abstractmethod
    def list_sources(self) -> List[str]:
        """
        List all sources of a catalog
        """
        raise NotImplementedError("You have to implement this.")

    @abc.abstractmethod
    def has_source(self, source_name: str) -> bool:
        """
        Checks, if source is available in catalog
        """
        raise NotImplementedError("You have to implement this.")

    @abc.abstractmethod
    def validate(self, data) -> bool:
        """
        Validate given data against catalog
        """
        raise NotImplementedError("You have to implement this.")

    def parse_transformations(
            self,
            transformations: List[Transformation]) -> None:
        for elem in transformations:
            print(elem)


class JsonCatalogProvider(CatalogProvider):
    """
    """
    def __init__(self, catalog: Dict[str, Any]) -> None:
        validate.is_in_dict_keys('namespace', catalog)
        validate.is_in_dict_keys('version', catalog)
        validate.is_in_dict_keys('sources', catalog)

        self.namespace: str = catalog.get('namespace')
        self.version: str = catalog.get('version')
        self.sources: dict = catalog.get('sources')

    def load(self, source_name: str) -> SourceDefinition:
        src = JsonCatalogProvider.__get_key_or_die(self.sources, source_name)
        source_config = JsonCatalogProvider.__get_key_or_die(src, 'source')
        target_config = JsonCatalogProvider.__get_key_or_die(src, 'target')
        parser_config = JsonCatalogProvider.__get_key_or_die(src, 'parser')
        fields = JsonCatalogProvider.__retrieve_fields(src)
        return SourceDefinition(
            name=source_name,
            source_config=source_config,
            target_config=target_config,
            parser_config=parser_config,
            version=self.version,
            fields=fields
        )

    @staticmethod
    def __retrieve_fields(source: Dict) -> Dict[str, Field]:
        validate.is_in_dict_keys('fields', source)
        fields = source.get('fields')
        res = dict()
        for field in fields:
            name = JsonCatalogProvider.__get_key_or_die(field, 'name')
            data_type = JsonCatalogProvider.__get_key_or_die(
                field,
                'data_type'
            )
            default = field.get('default', None)
            alias = field.get('alias', None)
            transformations = JsonCatalogProvider.__retrieve_transformations(
                field
            )
            res[name] = Field(
                name=name,
                data_type=data_type if data_type else None,
                default=default,
                alias=alias,
                transformations=transformations
            )
        return res

    @staticmethod
    def __retrieve_transformations(adict: Dict) -> Dict[str, Transformation]:
        """
        Pulls transformations out of a source and returns an OrderedDict

        Example of a :py:class:``Transformation``
        {
            "type": "rule",
            "task": {
              "name": "replace",
              "operator": {
                "old": " €",
                "new": ""
              }
            },
            "order": 1
        }
        """
        res = {}
        for elem in adict.get('transformations', []):
            type = JsonCatalogProvider.__get_key_or_die(elem, 'type')
            task = JsonCatalogProvider.__get_key_or_die(elem, 'task')
            order = elem.get('order')
            if TransformationType(type) == TransformationType.RULE:
                clz = TransformationFactory.load(
                    JsonCatalogProvider.__get_key_or_die(task, 'name')
                )
                rt = RuleTask(
                    name=JsonCatalogProvider.__get_key_or_die(task, 'name'),
                    operator=JsonCatalogProvider.__get_key_or_die(
                        task, 'operator')
                )
                res[order] = clz(task=rt, order=order)
            elif TransformationType(type) == TransformationType.FILTER:
                ft = FilterTask(
                    attribute=JsonCatalogProvider.__get_key_or_die(
                        task, 'attribute'),
                    operator=JsonCatalogProvider.__get_key_or_die(
                        task, 'operator'),
                    expression=JsonCatalogProvider.__get_key_or_die(
                        task, 'expression')
                )
                res[order] = FilterTransformation(task=ft, order=order)
            else:
                print("Wrong TrasformationType='{}'".format(type))

        return res

    @staticmethod
    def __get_key_or_die(dict: Dict, key: str) -> Any:
        if key not in dict:
            raise ConfigurationError(
                "There is no configuration node named '{}'. Abort".format(key)
            )
        return dict[key]

    def list_sources(self) -> List[str]:
        return [k for k, v in self.sources.items()]

    def has_source(self, source_name: str) -> bool:
        return source_name in self.sources.keys()

    def validate(self, data) -> bool:
        pass

    def __repr__(self) -> str:
        return "JsonCatalogProvider(namespace='{}', version='{}, sources={}".\
            format(self.namespace, self.version, self.sources)
