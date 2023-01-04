#!/bin/bash

RUN_LOCALLY="$1"

# Load dot env for the processor app
. .env

if [[ $RUN_LOCALLY =~ ^(locally)$ ]]; then
    # Overrides BACKEND_URL
    export BACKEND_URL=http://localhost:8080

    # Registers data sync service module to the python path
    export PYTHONPATH="$PWD/sync"
fi

python sync/main.py