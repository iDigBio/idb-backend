#!/usr/bin/env python3
import argparse
import feedparser

HR = "=" * 94

def s(x, default=""):
    """Return a safe printable string."""
    if x is None:
        return default
    if isinstance(x, bytes):
        return x.decode("utf-8", errors="replace")
    return str(x)

def get_field(entry, *names, default="NO VALUE FOUND"):
    for n in names:
        if n in entry and entry[n]:
            return s(entry[n])
    return default

def main():
    argparser = argparse.ArgumentParser(
        description="Dump dataset information out of an RSS feed in human-readable format."
    )
    argparser.add_argument("-f", "--feed", required=True, help="Filename or URL of RSS feed.")
    args = argparser.parse_args()
    feed_to_parse = args.feed

    if not feed_to_parse.startswith("http"):
        print()
        print("* non-HTTP feed supplied, assuming local file. *")

    feed = feedparser.parse(feed_to_parse)

    print()
    print(HR)
    print(feed_to_parse)

    feed_title = feed.get("feed", {}).get("title")
    if feed_title:
        print(s(feed_title))
    else:
        print("Feed has no TITLE.")

    print(HR)

    for entry in feed.entries:
        print("title:        ", get_field(entry, "title", default="NO TITLE FOUND").strip())
        print("published:    ", get_field(entry, "published", default="NO PUBLISHED DATE FOUND").strip())
        print("id:           ", get_field(entry, "id", default="NO ID FOUND").strip())
        print("dataset link: ", get_field(entry, "ipt_dwca", "link", default="NO DATASET LINK FOUND").strip())
        print("eml link:     ", get_field(entry, "ipt_eml", "emllink", default="NO EML LINK FOUND").strip())
        print(HR)

if __name__ == "__main__":
    main()
