# Troubleshooting & Common Errors

### (initial setup) Failed to get options via gdal-config: [Errno 2] No such file or directory

When running `pip install -r requirements.txt`:
> ERROR: Command errored out with exit status 1:  
> command: `python -c 'import sys, setuptools, tokenize; sys.argv[0] = '"'"'/tmp/pip-install-x26T8L/fiona/setup.py'"'"'; __file__='"'"'/tmp/pip-install-x26T8L/fiona/setup.py'"'"';f=getattr(tokenize, '"'"'open'"'"', open)(__file__);code=f.read().replace('"'"'\r\n'"'"', '"'"'\n'"'"');f.close();exec(compile(code, __file__, '"'"'exec'"'"'))' egg_info --egg-base /tmp/pip-pip-egg-info-auR3Qm`  
> &emsp;cwd: `/tmp/pip-install-x26T8L/fiona/`  
> Complete output (2 lines):  
> Failed to get options via gdal-config: [Errno 2] No such file or directory  
> A GDAL API version must be specified. Provide a path to gdal-config using a GDAL_CONFIG environment variable or use a GDAL_VERSION environment variable.  
> \----------------------------------------  
> ERROR: Command errored out with exit status 1: python setup.py egg_info Check the logs for full command output.

#### Cause

Missing dependency: libgdal-dev

#### Suggestion

Ensure all dependencies listed in [README.md: Installation&nbsp;> System Dependencies](README.md#system-dependencies) have been installed.

### (runtime, Elasticsearch) missing authentication token for REST request

When making HTTP requests to an Elasticsearch instance, the following HTTP 401 (Unauthorized) response is given:

```json
{
"error":{
  "root_cause":[{
    "type":"security_exception",
    "reason":"missing authentication token for REST request [/]",
    "header":{"WWW-Authenticate":"Basic realm=\"security\" charset=\"UTF-8\""}}],
  "type":"security_exception",
  "reason":"missing authentication token for REST request [/]",
  "header":{"WWW-Authenticate":"Basic realm=\"security\" charset=\"UTF-8\""}},
"status":401
}
```

#### Cause

Elasticsearch is set to reject anonymous requests

#### Suggestion

- Add appropriate credentials to your HTTP request.  
	For example, using _cURL_ and default Elasticsearch credentials:
	- `curl 'http://elastic:changeme@127.0.0.1:9200`
	- `curl -H 'Authorization: Basic ZWxhc3RpYzpjaGFuZ2VtZQ==' 'http://127.0.0.1:9200'`  
		(Following 'Authorization: Basic' is a base64 encoding of '\<username\>:\<password\>')
- **(not recommended in production)**
	- Disable Elasticsearch security features to allow unrestricted anonymous access by setting environment variable 'xpack.security.enabled' to 'false' or using the corresponding YAML configuration entry.
	- Specifically enable anonymous access:
		> see: "Elastic | Elasticsearch Guide | Enabling anonymous access",  
		> url: https://www.elastic.co/guide/en/elasticsearch/reference/current/anonymous-access.html  
		> accessed: 2023-11-17

#### Verification

If fixed, a request to the root URL (e.g. http://127.0.0.1:9200/) should display a response similar to:

```json
{
"name" : "PsxHpiQ",
"cluster_name" : "docker-cluster",
"cluster_uuid" : "7r4MW5ZXSZCGIVeuID1Kng",
"version" : {
  "number" : "5.5.3",
  "build_hash" : "9305a5e",
  "build_date" : "2017-09-07T15:56:59.599Z",
  "build_snapshot" : false,
  "lucene_version" : "6.6.0"
},
"tagline" : "You Know, for Search"
}
```

### (runtime, ingestion) `idigbio-ingestion update-publisher-recordset`: S3ResponseError: S3ResponseError: 501 Not Implemented

When running the above command:
> ERROR idb.storage჻ Failed operation on storage, attempt 1/3  
> Traceback (most recent call last):  
> &emsp;File "/home/idigbio-ingestion/idb-backend/idb/helpers/storage.py", line 158, in retry_loop  
> &emsp;&emsp;`return attemptfn()`  
> &emsp;File "/usr/local/lib/python2.7/site-packages/boto/s3/key.py", line 639, in make_public  
> &emsp;&emsp;`return self.bucket.set_canned_acl('public-read', self.name, headers)`  
> &emsp;File "/usr/local/lib/python2.7/site-packages/boto/s3/bucket.py", line 909, in set_canned_acl  
> &emsp;&emsp;`response.status, response.reason, body)`  
> S3ResponseError: S3ResponseError: 501 Not Implemented  
> ```xml
> <?xml version="1.0" encoding="UTF-8"?>
> <Error>
> <Code>NotImplemented</Code>
> <Message>A header you provided implies functionality that is not implemented ()</Message>
> <BucketName>idigbio-datasets-dev</BucketName>
> <Resource>/idigbio-datasets-dev/62c56d426f305f128ec10113d3df36f0</Resource>
> <RequestId>17BF24A270F5E21B</RequestId>
> <HostId>dd9025bab4ad464b049177c95eb6ebf374d3b3fd1af9251148b658df7ac2e3e8</HostId>
> </Error>
> ```

### Cause

Anonymous read access to S3 not allowed:
missing public-read access control list (ACL)

### Suggestion

Set the bucket named in the return XML (for example, 'idigbio-datasets-dev' above) to allow public read access.

If using MinIO, this can be done with the following steps:
1. Log in to MinIO (default link: http://127.0.0.1:9001 )
2. Go to bucket configuration page by clicking on 'Buckets' under the 'Administrator' section in the sidebar and selecting the problem bucket.
3. View the 'Anonymous' tab to manage anonymous access and click &lsqb;Add Access Rule&rsqb;
4. Use the following settings:  
	&emsp;**Prefix:** `/`  
	&emsp;**Access:** readonly  
	and click &lsqb;Save&rsqb;
5. Fix completed. Retry your `idigbio-ingestion` command.
