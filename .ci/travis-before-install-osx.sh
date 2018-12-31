#!/usr/bin/env bash

set -x

brew update
if [[ "${TARANTOOL_VERSION}" != "none" ]]; then
    if [[ "${TARANTOOL_VERSION}" == "2_x" ]]; then
        brew install .ci/tarantool.rb --HEAD
    else
        brew install tarantool
#        brew install .ci/tarantool.rb
    fi
fi
