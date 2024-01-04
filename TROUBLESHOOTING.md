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
