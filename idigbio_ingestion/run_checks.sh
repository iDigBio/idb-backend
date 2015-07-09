#!/bin/bash

export PYTHONPATH=$(dirname `pwd`)

python db_rsids.py | xargs -n1 -P `nproc` python db_check.py