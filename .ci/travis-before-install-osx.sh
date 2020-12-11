#!/usr/bin/env bash

set -e -x

export HOMEBREW_NO_AUTO_UPDATE=1

if [[ "${TARANTOOL_VERSION}" == "1.10" ]]; then
    brew install .ci/formulas/tarantool.rb
else
    brew install tarantool
fi

tarantool -V


brew install xz zlib pyenv || true
if ! (pyenv versions | grep "${PYTHON_VERSION}$"); then
    pyenv install ${PYTHON_VERSION}
fi
pyenv global ${PYTHON_VERSION}
pyenv rehash
eval "$(pyenv init -)"
