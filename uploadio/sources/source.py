from __future__ import annotations

from abc import abstractmethod
from typing import Any, Dict, Type, Union

import attr
import pandas as pd

from uploadio.utils import LogMixin


@attr.s
class Source(LogMixin):
    """ Provides data for a specific source """
    uri: str = attr.ib()
    data: Union[Dict[str, Any], pd.DataFrame] = attr.ib(init=False)
    options: Dict[str, Any] = attr.ib(default={})

    def load(self, uri: str = None, *args, **kwargs) -> Source:
        """
        :param uri: resource to load
            None: use URI defined in configuration
            Else: explicitly use different URI with options defined in config
        :param args: additional args
        :param kwargs: options
        :return: Source
        """
        if uri:
            self.uri = uri
        return self._load(uri, *args, **kwargs)

    @abstractmethod
    def _load(self, uri: str = None, *args, **kwargs) -> Source:
        raise NotImplementedError()


class DirectorySource(Source):

    def _load(self, uri: str = None, *args, **kwargs):
        regex = self.options.get('regex', '*')
        print(regex)
        return self


class CSVSource(Source):

    def _load(self, uri: str = None, *args, **kwargs) -> Source:
        self.options.update(**kwargs)
        self.data = pd.read_csv(filepath_or_buffer=self.uri, **self.options)
        return self


class XLSSource(Source):

    def _load(self, uri: str = None, *args, **kwargs) -> Source:
        self.options.update(**kwargs)
        self.data = pd.read_excel(io=self.uri, **self.options)
        return self


class JSONSource(Source):
    def _load(self,
              uri: str = None,
              *args,
              df: bool = False,
              **kwargs) -> Source:
        import json
        with open(self.uri, "r") as json_file:
            data = json.load(json_file)
            json_file.close()
        self.data = data if not df else pd.io.json.json_normalize(data, *args)
        return self


class HTTPSource(Source):
    """
    Downloads a file from a HTTP Uri and stores it on the filesystem
    """
    def _load(self,
              uri: str = None,
              *args,
              df: bool = False,
              **kwargs) -> Source:
        # validate.is_in_dict_keys('filename', self.options)
        # validate.is_in_dict_keys('resolver', self.options)

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

    def _load(self, uri: str = None, *args, **kwargs) -> Source:
        pass


class SourceFactory:

    __MAPPING: Dict[str, Type[Source]] = {
        "csv": CSVSource,
        "json": JSONSource,
        "http": HTTPSource,
        "xls": XLSSource
    }

    @staticmethod
    def __find(name: str) -> Type[Source]:
        # validate.is_in_dict_keys(name, SourceFactory.__MAPPING)
        return SourceFactory.__MAPPING[name]

    @classmethod
    def load(cls, config: Dict[str, Any]) -> Source:
        # validate.is_in_dict_keys('type', config)
        # validate.is_in_dict_keys('uri', config)
        src = SourceFactory.__find(config.get('type'))
        return src(
            uri=config.get('uri'),
            options=config.get('options', {})
        )
