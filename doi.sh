#!/bin/bash
source /condor/bmrbgrid/doi_assign/env/bin/activate
/condor/bmrbgrid/doi_assign/assign.py "$@"
