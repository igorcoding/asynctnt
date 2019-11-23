#!/usr/bin/env bash

set -e -x

if [[ "${BUILD}" != *tests* ]]; then
    echo "Skipping tests."
    exit 0
fi

source .ci/common-${TRAVIS_OS_NAME}.sh

if [[ "${BUILD}" == *quicktests* ]]; then
    make && make quicktest
else
    make && make test
    make clean && make debug && make test
fi

if [[ "${BUILD}" == *coverage* ]]; then
    make debug && coverage run --source=asynctnt setup.py test
    coveralls
fi
