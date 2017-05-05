from __future__ import absolute_import, print_function

import argparse
import requests
import feedparser
assert feedparser.__version__ >= "5.2.0"


def get_name_from_rss(rss_feed_url):
    print ('WORKING on {0}'.format(rss_feed_url))
    try:
        req = requests.get(rss_feed_url, timeout=5)
        parsed = feedparser.parse(req.content)
        return parsed['feed']['title']
    except:
        return 'NO_TITLE'

def writeline (handle):
    handle.write("\n")

def main():
    argparser = argparse.ArgumentParser(description='Check all Publisher records against RSS feeds to instances that need to be updated in the db.')
    #argparser.add_argument("-p", "--publisher", required=False, help="Check single publisher only, by uuid")
    argparser.add_argument("-o", "--output", required=True, help="output file path")
    args = argparser.parse_args()

    #publisher_uuid = args.publisher
    output_filename = args.output


    if output_filename:
        print ("Output will be written to " + output_filename)
        output_file = open(output_filename, "w")
    else:
        raise

    search_url = 'http://search.idigbio.org/v2/search/publishers/?fields=[%22uuid%22,%22name%22,%22indexData.rss_url%22]&limit=1000'

    r = requests.get(search_url)

    request_json = r.json()

    matched = {}

    for item in request_json["items"]:
        u = item["uuid"]
        n = item["indexTerms"]["name"]
        r = item["indexTerms"]["indexData"]["rss_url"]
        title = get_name_from_rss(r)
        if n != title.lower():
            status = 'DIFFERENT'
        else:
            status = 'SAME'
        matched[u] = (status,n,r,title)

    output_file.write("status,pub_name,rss_url,title")
    writeline(output_file)

    for each in matched:
        line = matched[each][0].encode("utf-8") + ',' + '"' + matched[each][1].encode("utf-8") + '","' + matched[each][2].encode("utf-8") + '","' + matched[each][3].encode("utf-8") + '"'
        output_file.write(line)
        writeline(output_file)

    print ("Output written to " + output_filename)

if __name__ == '__main__':
    main()
