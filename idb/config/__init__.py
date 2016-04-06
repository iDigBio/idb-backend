import os
import sys
import json

import logging

logging.basicConfig(
    format="%(asctime)s %(levelname)-5.5s [%(name)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S")


def getIDigBioLogger(name=""):
    logname = name
    if logname == "":
        logname = "idigbio"
    logger = logging.getLogger(logname)
    return logger

logger = getIDigBioLogger()

conf_paths = ["/etc/idigbio/", "~/", "."]

config = {}

ENV = 'dev'

for p in conf_paths:
    p = os.path.abspath(os.path.expanduser(p)) + "/"
    if os.path.exists(p + "idigbio.json"):
        with open(p + "idigbio.json", "rb") as conf:
            config.update(json.load(conf))

if "env" in config:
    for k in config["env"]:
        if k not in os.environ:
            os.environ[k] = config["env"][k]
    ENV = config['env']['ENV']
