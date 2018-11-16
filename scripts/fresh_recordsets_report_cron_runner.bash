#!/bin/bash
# RUNS the fresh-recordsets-report suitable for cron, emails the results
set -e

# change to the directory where this script is running
cd ${0%/*}

pwd # confirm we are in the proper dir

now=`date -I`

echo Starting python - Fresh Recordsets Report generation...

python fresh-recordsets-report.py -f csv  # makes a csv named fresh-recordsets-report.csv

echo Starting zip...
zip -r fresh-recordsets-report.zip fresh-recordsets-report.csv

echo Starting mail command...

mailx -v -r idigbio@acis.ufl.edu -a fresh-recordsets-report.zip -s "Fresh Recordsets Report for $now" data@idigbio.org <<EOF
Data report attached.
EOF

echo Removing temporary files...
rm fresh-recordsets-report.csv
rm fresh-recordsets-report.zip

echo Finished run.
