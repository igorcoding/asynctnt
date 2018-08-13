#!/usr/bin/env bash

echo "TRAVIS_OS_NAME = ${TRAVIS_OS_NAME}"

set -x

.ci/travis-before-install-${TRAVIS_OS_NAME}.sh || exit 1
.ci/travis-install.sh || exit 1
.ci/travis-tests.sh || exit 1
.ci/travis-build-docs.sh || exit 1
