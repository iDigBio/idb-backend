#!/bin/bash

DIR=$( dirname "${BASH_SOURCE[0]}" )
export PYTHONPATH=$( dirname "${DIR}" )

python $DIR/db_rsids.py | xargs -n1 -P `nproc` python $DIR/db_check.py $@