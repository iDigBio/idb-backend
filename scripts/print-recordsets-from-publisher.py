# coding: utf-8
from __future__ import absolute_import

import argparse
import requests


argparser = argparse.ArgumentParser(description='Print list of recordsets that belong to a Publisher via iDigBio Search API')
argparser.add_argument("-p", "--publisher", required=True, help="uuid of the publisher")
argparser.add_argument("-o", "--output", required=True, help="output file path")
args = argparser.parse_args()

publisher_uuid = args.publisher
output_filename = args.output

print ("Output will be written to " + output_filename)
output_file = open(output_filename, "w")

def writeline (handle):
	handle.write("\n")

search_url = 'http://search.idigbio.org/v2/search/recordsets/?rsq={%22publisher%22:%22' + \
    publisher_uuid + \
    '%22}&fields=[%22name%22,%20%22publisher%22]&limit=1000'

r = requests.get(search_url)

request_json = r.json()

matched = {}

for item in request_json["items"]:
        u = item["uuid"]
        n = item["indexTerms"]["name"]
        matched[u] = n

for each in matched:
    rs_url = 'https://www.idigbio.org/portal/recordsets/' + each
    output_file.write(matched[each].encode("utf-8"))
    writeline(output_file)
    output_file.write (rs_url)
    writeline(output_file)
    writeline(output_file)
    

