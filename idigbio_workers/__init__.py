import os
import sys
from celery import Celery

app = Celery('tasks')
env = os.getenv("ENV", "prod")
app.config_from_object('idigbio_workers.config.' + env)

from tasks.download import downloader, send_download_email  # noqa
