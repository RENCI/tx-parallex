#!/bin/bash
PYTHONPATH=src LOG_LEVEL=WARNING python profile/profile.py ${1:-16} ${2:-wall}
