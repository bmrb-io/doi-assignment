#!/bin/bash

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" >/dev/null 2>&1 && pwd)"

python -m virtualenv ${SCRIPT_DIR}/env
source ${SCRIPT_DIR}/env/bin/activate
pip install -r requirements.txt
