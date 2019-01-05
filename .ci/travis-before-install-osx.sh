#!/usr/bin/env bash

set -e -x

#brew update
if [[ "${TARANTOOL_VERSION}" == "2_x" ]]; then
    brew install .ci/tarantool.rb --HEAD
else
    brew install tarantool
fi
