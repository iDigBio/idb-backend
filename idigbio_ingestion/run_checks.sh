#!/bin/bash

# Should run from the data directory, typically /mnt/data/new_ingestion

DIR=$( dirname "${BASH_SOURCE[0]}" )
export PYTHONPATH=$( dirname "${DIR}" )

python $DIR/db_rsids.py | xargs -n1 -P `nproc` python $DIR/db_check.py $@

echo "Generating... summary.csv"
python $DIR/ds_sum_counts.py /mnt/data/new_ingestion/ > summary.csv

echo "Converting summary.csv to columnar human readable report... summary.pretty.txt"
column -ts ',' summary.csv | sort > summary.pretty.txt
