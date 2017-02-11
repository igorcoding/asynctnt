#!/usr/bin/env bash

$@; while [ $? -ne 0 ]; do $@; done
