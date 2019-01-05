#!/usr/bin/env bash

set -e -x

curl -L https://packagecloud.io/tarantool/${TARANTOOL_VERSION}/gpgkey | sudo apt-key add -
release=`lsb_release -c -s`
sudo apt-get -y install apt-transport-https
sudo rm -f /etc/apt/sources.list.d/*tarantool*.list
echo "deb https://packagecloud.io/tarantool/${TARANTOOL_VERSION}/ubuntu/ $release main" | sudo tee /etc/apt/sources.list.d/tarantool_${TARANTOOL_VERSION}.list
echo "deb-src https://packagecloud.io/tarantool/${TARANTOOL_VERSION}/ubuntu/ $release main" | sudo tee -a /etc/apt/sources.list.d/tarantool_${TARANTOOL_VERSION}.list
sudo apt-get -qq update

sudo apt-get -y install libssl-dev openssl tarantool

tarantool -V
