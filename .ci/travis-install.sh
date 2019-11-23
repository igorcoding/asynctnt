#!/usr/bin/env bash

set -e -x

source .ci/common-${TRAVIS_OS_NAME}.sh

pip install --upgrade pip setuptools coveralls
pip install -r requirements.txt
pip install -e .
