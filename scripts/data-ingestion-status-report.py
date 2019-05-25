import idigbio
import locale
import requests
import os
import argparse
import datetime

locale.setlocale(locale.LC_ALL, 'en_US.UTF-8')

### Run with:
### $ GITHUB_ACCESS_TOKEN=*your_github_access_token* python data_ingestion_status_report.py

since_date = None
parser = argparse.ArgumentParser(description='generate monthly Data Ingestion status report')
parser.add_argument('--since', dest='since_date', required=False, help='The "since" date for github issues reporting in the form of YYYY-MM-DD')
args = parser.parse_args()

if since_date is not None:
    # verify here that the supplied date argument is an actual date
    try:
        since_date = datetime.datetime.strftime(datetime.datetime.strptime(args.since_date, '%Y-%m-%d'),'%Y-%m-%d')
    except:
        print '''
*****************************************************************
ERROR:  since_date must be supplied in the format --> YYYY-MM-DD
*****************************************************************
'''
        raise SystemExit


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

# Generate VertNet section

# buff = buff + "VertNet Report since " + since_date + ":\n\n"
#
# buff = buff + "(github login and access permission are required to view issues)\n\n"
# if os.environ.get('GITHUB_ACCESS_TOKEN') is None:
#     buff = buff + "*Unable to access VertNet github issues to generate report. Check environment variables.* \n\n"
# else:
#     access_token = os.environ.get('GITHUB_ACCESS_TOKEN')
#     api_priv_url = 'https://api.github.com/repos/VertNet/Tasks/issues'
#     q_month = '?since=' + since_date
#
#     r = requests.get(api_priv_url + q_month, headers={'Authorization': 'token ' + access_token})
#
#     buff = buff + "NEW issues:\n"
#     for each in r.json():
#         if "title" in each:
#             for labels in each["labels"]:
#                 if "name" in labels:
#                     if labels["name"] == "new":
#                         buff = buff + each["html_url"] + "  " + each["title"] + "\n"
#
#     ## leave UPDATED out of report for now.
#     #buff = buff + "\nUPDATED issues:\n"
#     #for each in r.json():
#     #    if "title" in each:
#     #        buff = buff + each["html_url"] + "  " + each["title"] + "\n"

    
#buff = buff + "\n" + hr

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

