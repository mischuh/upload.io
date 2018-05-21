#!/bin/bash

# Resolve base directory
BASEDIR=$(dirname "$0")
ABS_BASEDIR=$(cd $(dirname $BASEDIR); pwd)

# Resolve PATH for PYTHON PATH
MY_PATH=$ABS_BASEDIR
# Resolve the actual test directory
TEST_FOLDER=test/
# Main folder
MAIN_FOLDER=$ABS_BASEDIR/test

(export PYTHONPATH="$MY_PATH"; cd $TEST_FOLDER; py.test -v "$@")
