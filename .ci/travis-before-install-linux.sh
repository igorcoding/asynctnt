#!/usr/bin/env bash

set -e -x

sudo apt-get -y install gnupg2 curl lsb-release apt-transport-https
curl https://download.tarantool.org/tarantool/release/${TARANTOOL_VERSION}/gpgkey | sudo apt-key add -
release=`lsb_release -c -s`
sudo rm -f /etc/apt/sources.list.d/*tarantool*.list
echo "deb https://download.tarantool.org/tarantool/release/${TARANTOOL_VERSION}/ubuntu/ ${release} main" | sudo tee /etc/apt/sources.list.d/tarantool.list
echo "deb-src https://download.tarantool.org/tarantool/release/${TARANTOOL_VERSION}/ubuntu/ ${release} main" | sudo tee -a /etc/apt/sources.list.d/tarantool.list
sudo apt-get -qq update
sudo apt-get -y install libssl-dev openssl tarantool

tarantool -V
