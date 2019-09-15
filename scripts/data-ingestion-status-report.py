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


Approximately ___ recordsets were re-published by data providers with updated data.

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


print "***** BEGIN *****"

print buff

print "***** END *****"
