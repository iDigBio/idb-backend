from __future__ import print_function
import feedparser
from pyquery import PyQuery as pq
import argparse

argparser = argparse.ArgumentParser(description='Script to quickly dump dataset information out of an RSS feed in human-readable format.')
argparser.add_argument("-f", "--feed", required=True, help="The filename or URL of the RSS feed to parse.")
args = argparser.parse_args()
feed_to_parse = args.feed

if not feed_to_parse.startswith('http'):
    print ()
    print ("* non-HTTP feed supplied, assuming local file. *")

feed = feedparser.parse(feed_to_parse)


def get_title(entry):
    if "title" in entry:
        return(entry["title"])
    else:
        return("NO TITLE FOUND")

def get_pubDate(entry):
    if "published" in entry:
        return (entry["published"])
    else:
        return ("NO PUBLISHED DATE FOUND")


def get_id(entry):
    if "id" in entry:
        return (entry["id"])
    else:
        return ("NO ID FOUND")

def get_dataset_link(entry):
    if "ipt_dwca" in entry:
        return (entry["ipt_dwca"])
    elif "link" in entry:
        return (entry["link"])
    else:
        return ("NO DATASET LINK FOUND")

def get_eml_link(entry):
    if "ipt_eml" in entry:
        return (entry["ipt_eml"])
    elif "emllink" in entry:
        return (entry["emllink"])
    else:
        return ("NO EML LINK FOUND")


hr = "=============================================================================================="

print () 
print (hr)
print (feed_to_parse)

if feed.bozo:
    raise feed.bozo_exception

if "title" in feed['feed']:
    print (feed['feed']['title'])
else:
    print ("Feed has no TITLE.")

print (hr)
for entry in feed.entries:
    entry_title = ""
    entry_pubDate = ""
    entry_id = ""
    entry_dataset_link = ""
    entry_eml_link = ""
    # feedparser converts many common fields into normalized names. Examples:
    #   guid --> id
    #   pubDate --> published
    #
    # Fields that contain colons such as ipt:dwca and ipt:eml get underscored to ipt_dwca and ipt_eml
    #
    # The actual IPT guid field is not visible as a normalized field since another id field is used.
    # However, the id is embedded in the middle of the id url so human can pluck it out if needed.


    print ("title:        ", get_title(entry).encode('utf-8').strip())
    print ("published:    ", get_pubDate(entry).encode('utf-8').strip())
    print ("id:           ", get_id(entry).encode('utf-8').strip())
    print ("dataset link: ", get_dataset_link(entry).encode('utf-8').strip())
    print ("eml link:     ", get_eml_link(entry).encode('utf-8').strip())
    print (hr)

