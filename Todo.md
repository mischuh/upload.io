# TODOs

*Short term:*

* Testtestestesttest
* Make _URI_ in source optional, so we must inject a file if this item is missing in config
* **Source data must be pandas.DataFrame**
* Switch to YAML configs
* Think about connection pooling...
* Schema validation
  * [Schema](https://github.com/keleshev/schema)
  * specifically `Collection` and `Catalog`
* ~~DB output API~~
  * solution is an abstraction layer with sqlalchemy engine
    ~~* sqlite is ok for the beginning...~~
    ~~* postgres~~
* DictionarySource, load a bunch of files
  * in particular in combination with watchdog
* RegExSource
* ~~Scheduler for repeating tasks~~
  * [Watchdog](https://pypi.org/project/watchdog/)
    * http://brunorocha.org/python/watching-a-directory-for-file-changes-with-python.html

*Medium term:*

* Plugable ``Source`` packaging so one can easily extend sources
* Docker infrastucture to launch a proper setting

*Long term:*

* Fancy UI to configure a catalog/pipeline
