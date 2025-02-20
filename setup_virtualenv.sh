#!/bin/bash

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" >/dev/null 2>&1 && pwd)"

python3 -m venv "${SCRIPT_DIR}"/venv
source "${SCRIPT_DIR}"/venv/bin/activate
pip3 install -r requirements.txt
