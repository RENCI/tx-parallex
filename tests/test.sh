#!/bin/bash
LOG_LEVEL=DEBUG PYTHONPATH=src pytest -x -vv --full-trace -s --timeout 10 "$@"
