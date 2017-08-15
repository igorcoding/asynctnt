#!/usr/bin/env bash

set -e -x

if [ "${TRAVIS_OS_NAME}" == "linux" ]; then
    echo "linux"
elif [ "${TRAVIS_OS_NAME}" == "osx" ]; then
    git clone https://github.com/yyuu/pyenv.git ~/.pyenv
    PYENV_ROOT="$HOME/.pyenv"
    PATH="$PYENV_ROOT/bin:$PATH"
    eval "$(pyenv init -)"

    if ! (pyenv versions | grep "${PYTHON_VERSION}$"); then
        pyenv install ${PYTHON_VERSION}
    fi
    pyenv global ${PYTHON_VERSION}
    pyenv rehash
fi

pip install --upgrade pip wheel
pip install --upgrade setuptools
pip install -r requirements.txt
pip install coveralls
pip install -e .
