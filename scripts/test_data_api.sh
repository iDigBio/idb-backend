#!/bin/bash
# Script to do basic testing of data-api 

# requires httpie installed
http --version
# test for special value returned when command not found
if [ $? -eq 127 ]
then 
    echo "Error: httpie must be involved."
    echo "install via: sudo apt install httpie"
else
    echo "httpie is installed, continuing"
fi
# test all the routes of the data API to be sure they respond
# reference: idb-backend/blob/master/idb/data_api/config.py
SUPPORTED_TYPES=( "recordsets" "records" "publishers" "mediarecords" )
base_url="api.idigbio.org"
api_version="v2"

if [ "$1" == 'beta' ]
then
    env='beta'
    hostname='beta-api'
else
if [ "$1" == 'prod' ]
then
    env='prod'
    hostname='api'
else
    echo "Error: please specify prod or beta environment for testing"
    echo "call script like this: ./test_data_api.sh beta"
    exit
fi
fi

route="view"
testing_route="${hostname}.idigbio.org/$api_version/$route/"
for i in "${SUPPORTED_TYPES[@]}"
do
    testing_url=${testing_route}$i
    echo "testing $testing_url"
    if http  --check-status --ignore-stdin $testing_url &> /dev/null; 
    then
        echo 'OK!'
    else
        case $? in
            2) echo 'Request timed out!' ;;
            3) echo 'Unexpected HTTP 3xx Redirection!' ;;
            4) echo 'HTTP 4xx Client Error!' ;;
            5) echo 'HTTP 5xx Server Error!' ;;
            6) echo 'Exceeded --max-redirects=<n> redirects!' ;;
            *) echo 'Other Error!' ;;
        esac
    fi
done
echo "Completed testing."