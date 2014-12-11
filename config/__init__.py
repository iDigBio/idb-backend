import os
import sys
import json

conf_paths = ["/etc/idigbio/","~/", "."]

config = {}

for p in conf_paths:
    p = os.path.abspath(os.path.expanduser(p)) + "/"
    if os.path.exists(p + "idigbio.json"):
        with open(p + "idigbio.json", "rb") as conf:
            config.update(json.load(conf))

if "env" in config:
    for k in config["env"]:
        os.environ[k] = config["env"][k]