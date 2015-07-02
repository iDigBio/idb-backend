import os
import sys
from celery import Celery

mybase = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))
if mybase not in sys.path:
    sys.path.append(mybase)

print sys.path

app = Celery('tasks')
env = os.getenv("ENV", "prod")
app.config_from_object('idigbio_workers.config.' + env)

from tasks.download import downloader