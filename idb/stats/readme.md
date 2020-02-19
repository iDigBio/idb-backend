# stats

IDB staff should consult the following for more information: https://redmine.idigbio.org/projects/infrastructure/wiki/Portal_and_api_stats_overview

**Warning: these processes are not idempotent.  Some efforts are present in the code to prevent duplicate runs but care should still be taken.**

## telemetry/interaction stats

These come from the search api and are things such as "search," "view," and "download."

Aggregation of telemetry stats is done by systemd job fired by systemd timer, `idb-interaction-telemetry-aggregator` (in this repo).  The code lives [here](https://github.com/iDigBio/idb-backend/blob/master/idb/stats/collect.py).


for telemtry stats, it defaults to `now()` back to 24 hours previously.  To pass another date (but still hard-coded to the "24 hours previous" limit):
```
idb -vv collect-stats -d 2020-02-18
```

The stats process for telemetry/interaction data fetches the previous totals out of elasticsearch, loads the new data from postgres, aggregates everything, and puts it back in elasticsearch.

An example of this aggregator structure can be found in `example-objects/stats-aggregation-object-structure-single.json`.  In this case, it would be a single "view" action.  Note that this document has placeholders for recordset_uuid and record_uuid in order to help understand the structure.

`example-objectsstats-dict-object.zip` is a larger (1.8MB json uncompressed) example of the data that is being saved into elasticsearch.

## recordset stats

Aggregation of recordset stats is done by systemd job fired by systemd timer, `idb-recordset-stats-aggregator` (in this repo).  The code lives [here](https://github.com/iDigBio/idb-backend/blob/master/idb/stats/collect.py).

This loads: 

```
SELECT parent,type,count(id)
FROM uuids
WHERE deleted=false and (type='record' or type='mediarecord')
```

and then aggregates them into final structure [like so](recordset-aggregation-structure.txt)

----

## installation

create a non-root user.  We will run this service out of this user's home directory.

### as non-root user

install virtualenv and virtualenvwrapper:
```
pip install virtualenv
pip install virtualenvwrapper
```

try running `lsvirtualenv`.  If you get complaints, you'll need to create a `.bash_profile` or add the following to the user's existing profile:
```
$ cat ~/.bash_profile 
# set where virutal environments will live
export WORKON_HOME=$HOME/.virtualenvs
# ensure all new environments are isolated from the site-packages directory
# export VIRTUALENVWRAPPER_VIRTUALENV_ARGS='--no-site-packages'
# use the same directory for virtualenvs as virtualenvwrapper
export PIP_VIRTUALENV_BASE=$WORKON_HOME

# pip should only run if there is a virtualenv currently activated
export PIP_REQUIRE_VIRTUALENV=true

# cache pip-installed packages to avoid re-downloading
export PIP_DOWNLOAD_CACHE=$HOME/.pip/cache

# makes pip detect an active virtualenv and install to it
export PIP_RESPECT_VIRTUALENV=true
if [[ -r /usr/local/bin/virtualenvwrapper.sh ]]; then
    source /usr/local/bin/virtualenvwrapper.sh
else
    echo "WARNING: Can't find virtualenvwrapper.sh"
fi
```

if you had to create/modify your bash profile, make sure to load it:

```
source ~/.bash_profile 
```

create a virtualenv for the stats/backend and active it:
```
mkvirtualenv idb-backend
workon idb-backend
```


install the idb backend according to the readme.  For "stats only", you can do:

```
pip install -e .[journal]
```

for a user "stats" and a virtualenv named "idb-backend", the path to `idb` will look like this:
```
$ which idb
/home/stats/.virtualenvs/idb-backend/bin/idb
```

type `idb` and you should get the default help screen.  If so, it's been installed successfully.

### as root

as root, copy the systemd files into `/etc/systemd/system` and then reload to pick them up:

```
systemctl daemon-reload
```

to start manually:
```
systemctl start idb-interaction-telemetry-aggregator.service
```

to enable on timer:
```
systemctl enable idb-interaction-telemetry-aggregator.timer
```

Note that in the service files, we're running the service "as" the non-root user and running it out of that virutalenv that we created.  The virtualenv is "baked into" the python interpreter in that env, which means that we don't have to active the virtualenv first.
