#!/usr/bin/env bash

$@; while [ $? -eq 0 ]; do $@; done
