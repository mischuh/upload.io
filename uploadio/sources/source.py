from abc import abstractmethod
from typing import Dict, Any, Union, Type

import pandas as pd

from src.p3common.common import validators as validate
from uploadio.utils import Loggable


class Source(Loggable):
    """ Provides data for a specific source """

    def __init__(self, uri: str, **options) -> None:
        self.uri = uri
        self.options: Dict[str, Any] = {} if not options else options
        self.data: Union[Dict[str, Any], pd.DataFrame] = None

    def load(self, *args, **kwargs) -> Any:
        return self._load(*args, **kwargs)

    @abstractmethod
    def _load(self, *args, **kwargs) -> Any:
        raise NotImplementedError()


class DirectorySource(Source):

    def _load(self, *args, **kwargs):
        regex = self.options.get('regex', '*')
        print(regex)
        return self


class CSVSource(Source):

    def _load(self, *args, **kwargs) -> Source:
        self.options.update(**kwargs)
        self.data = pd.read_csv(filepath_or_buffer=self.uri, **self.options)
        return self


class JSONSource(Source):

    def _load(self, *args, df: bool=False, **kwargs) -> Source:
        import json
        with open(self.uri, "r") as json_file:
            data = json.load(json_file)
            json_file.close()
        self.data = data if not df else pd.io.json.json_normalize(data, *args)
        return self
        # return pd.io.json.json_normalize(data, *args)


class HTTPSource(Source):
    """
    Downloads a file from a HTTP Uri and stores it on the filesystem
    """
    def _load(self, *args, df: bool=False, **kwargs) -> Source:
        validate.is_in_dict_keys('filename', self.options)
        validate.is_in_dict_keys('resolver', self.options)

        import requests
        filename = self.options.get('filename')
        self.logger.info(f"HTTPSource: Downloading file {filename}")
        req = requests.get(self.uri)
        file = open(filename, 'wb')
        for chunk in req.iter_content(100000):
            file.write(chunk)
        file.close()
        options = dict(uri=filename, type=self.options.get('resolver'))
        return SourceFactory.load(options).load()


class StreamSource(Source):

    def _load(self, *args, **kwargs) -> Source:
        pass


class SourceFactory:

    __MAPPING: Dict[str, Type[Source]] = {
        "csv": CSVSource,
        "json": JSONSource,
        "http": HTTPSource
    }

    @staticmethod
    def __find(name: str) -> Type[Source]:
        validate.is_in_dict_keys(name, SourceFactory.__MAPPING)
        return SourceFactory.__MAPPING[name]

    @classmethod
    def load(cls, config: Dict[str, Any]) -> Source:
        validate.is_in_dict_keys('type', config)
        validate.is_in_dict_keys('uri', config)
        src = SourceFactory.__find(config.get('type'))
        return src(
            uri=config.get('uri'),
            **config.get('options', {})
        )
