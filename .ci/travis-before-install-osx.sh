#!/usr/bin/env bash

set -e -x

brew update

if [[ "${TARANTOOL_VERSION}" == "1_10" ]]; then
    brew install .ci/formulas/tarantool.rb
    brew reinstall .ci/formulas/icu4c.rb
    brew switch icu4c 62.1
    brew uninstall --ignore-dependencies openssl
    brew reinstall .ci/formulas/openssl.rb
    ln -s /usr/local/opt/openssl/include/openssl /usr/local/include
    ln -s /usr/local/opt/openssl/lib/libssl.1.0.0.dylib /usr/local/lib/
else
    brew install tarantool
fi

tarantool -V


brew install xz zlib pyenv || echo "ignore"
if ! (pyenv versions | grep "${PYTHON_VERSION}$"); then
    pyenv install ${PYTHON_VERSION}
fi
pyenv global ${PYTHON_VERSION}
pyenv rehash
eval "$(pyenv init -)"
