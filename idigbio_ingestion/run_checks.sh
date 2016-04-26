#!/bin/bash

# Must run from the data directory, typically /mnt/data/new_ingestion

DIR=$( dirname "${BASH_SOURCE[0]}" )
export PYTHONPATH=$( dirname "${DIR}" )

python $DIR/db_rsids.py | xargs -n1 -P `nproc` python $DIR/db_check.py $@

echo "Generating... summary.csv"
python $DIR/ds_sum_counts.py ./ > summary.csv
python $DIR/ds_suspects.py ./ summary.csv suspects.csv

echo "Converting summary.csv (all recordsets subject to ingestion) to columnar human readable report... summary.pretty.txt"
column -ts ',' summary.csv | sort > summary.pretty.txt
echo "Converting suspects.csv (recordsets with questionable updates) to columnar human readable report... suspects.pretty.txt"
column -ts ',' suspects.csv | sort > suspects.pretty.txt
