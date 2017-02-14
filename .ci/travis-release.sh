#!/usr/bin/env bash

set -e -x


if [ -z "${TRAVIS_TAG}" ]; then
    # Not a release
    exit 0
fi


python setup.py sdist
twine upload dist/*
