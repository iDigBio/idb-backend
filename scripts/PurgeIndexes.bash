#!/bin/bash

set -x

for i in `curl -s c20node2:9200/_cat/indices | cut -d ' ' -f 3 | sort | grep -vE '(idigbio|stats|taxonnames|kibana)'`; do curl -X DELETE c20node2:9200/$i ; done
