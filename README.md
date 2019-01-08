# Upload.io

The basic idea is that you have a source and a target and a mapping in between
and a catalog that holds all the information.

You can find simple exmaples here:
    python3 sandbox.py

First try to use static typing in python. Further more this is a playground to glue different
components together (to name a few: load a csv, convert it into events, put it on queue and
do some stream processing on it)...

## Run example

    python3 ./runner.py -p /tmp -c ./resources/catalog_auszug.json -s kontoauszug

## Build examlpe container

    make docker
    docker run -v DIRECTORY_TO_OBSERVER:watchdog --net=host uploadio:0.0.1

## Run tests

Run all tests

    make test

If you just want to test a specific tests, do as follows:

    export PYTHONPATH=`pwd`
    pytest -s test/sources/test_transformation.py::test_filter_on_df uploadio/