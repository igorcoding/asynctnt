#!/usr/bin/env bash

set -x

echo "TRAVIS_OS_NAME = ${TRAVIS_OS_NAME}"

if [[ "${TRAVIS_OS_NAME}" == "linux" ]]; then
    sudo systemctl mask apt-daily.service
    sudo systemctl mask apt-daily.timer
    sudo systemctl mask apt-daily-upgrade.service
    sudo systemctl mask apt-daily-upgrade.timer
    bash ./.ci/wait-for-apt.sh

    curl -L https://packagecloud.io/tarantool/${TARANTOOL_VERSION}/gpgkey | sudo apt-key add -
    release=`lsb_release -c -s`
    sudo apt-get -y install apt-transport-https
    sudo rm -f /etc/apt/sources.list.d/*tarantool*.list
    echo "deb https://packagecloud.io/tarantool/${TARANTOOL_VERSION}/ubuntu/ $release main" | sudo tee /etc/apt/sources.list.d/tarantool_${TARANTOOL_VERSION}.list
    echo "deb-src https://packagecloud.io/tarantool/${TARANTOOL_VERSION}/ubuntu/ $release main" | sudo tee -a /etc/apt/sources.list.d/tarantool_${TARANTOOL_VERSION}.list
    sudo apt-get -qq update
    sudo apt-get -y install tarantool
    sudo tarantoolctl stop example || exit 0
    sudo apt-get install pandoc libssl-dev openssl
elif [[ "${TRAVIS_OS_NAME}" == "osx" ]]; then
    brew update
    if [[ "${TARANTOOL_VERSION}" == "2_0" ]]; then
        brew install tarantool --HEAD
    else
        brew install tarantool
    fi
fi
