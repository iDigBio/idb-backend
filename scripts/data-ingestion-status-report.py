from __future__ import print_function
import idigbio
import locale
import requests
import os
import argparse
import datetime

locale.setlocale(locale.LC_ALL, 'en_US.UTF-8')

parser = argparse.ArgumentParser(description='generate monthly Data Ingestion status report')
args = parser.parse_args()

buff = ''

hr = '================================================================\n'

buff = buff + hr

buff = buff + '''
DATA INGESTION STATUS REPORT
'''

buff = buff + datetime.date.today().strftime("%B %d, %Y") + "\n\n"
buff = buff + hr + "\n"

api = idigbio.json()
record_count = locale.format("%d", api.count_records(), grouping=True)
media_record_count = locale.format("%d", api.count_media(), grouping=True)
recordset_count = locale.format("%d", api.count_recordsets(), grouping=True)

# Updated recordsets is an approximation based on the number of items
# appearing in the Ingestion summary log, minus 15% to take into account
# that we usually overlap ingest periods for db-check operations by a few weeks.
# Currently the summary is in an expected, hard-coded path:
#   /mnt/data/new_ingestion/summary.pretty.txt
summary_path = "/mnt/data/new_ingestion/summary.pretty.txt"
if os.path.exists(summary_path):
    linecount = len(open(summary_path).readlines())
    updated_recordsets_blurb = ("Approximately {0}".format(int(linecount * .85)) +
        " recordsets were re-published by data providers with updated data.")
else:
# If we cannot find the summary file, use a generic madlibs line
    updated_recordsets_blurb = ("Approximately ___ " +
        "recordsets were re-published by data providers with updated data.")

buff = buff + 'COUNTS\n\n'
buff = buff + 'Recordsets: ' + str(recordset_count) + "\n"
buff = buff + 'Records: ' + record_count + "\n"
buff = buff + 'Media Records: ' + str(media_record_count) + "\n\n"


buff = buff + hr

buff = buff + '''
NEW Publishers and/or Recordsets added to iDigBio since last report:

*insert recordsets here*


'''

buff = buff + hr + "\n"


buff = buff + '''
UPDATED Recordsets since last report:

'''

buff = buff + updated_recordsets_blurb + "\n" + '''
These data were incorporated into iDigBio by the standard Data Ingestion process.

'''

buff = buff + hr + "\n"

buff = buff + '''
Notes:


*insert notes here*

'''

buff = buff + '''

Links:

(public)
https://www.idigbio.org/portal/publishers?merged=1
https://www.idigbio.org/wiki/index.php/Data_Ingestion_Report

(login required)
https://www.idigbio.org/redmine/projects/data-mobilization/issues
https://github.com/vertnet/tasks

'''

buff = buff + hr


print ("***** BEGIN *****")

print (buff)

print ("***** END *****")
