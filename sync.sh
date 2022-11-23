#!/bin/bash

RUN_LOCALLY="$1"

if [[ $RUN_LOCALLY =~ ^(locally)$ ]]; then
    . .env

    export PYTHONPATH="$PWD/sync"
fi

python sync/main.py