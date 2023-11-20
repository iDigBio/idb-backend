#NOTE: password supplied in broker_url is ignored;
# in idigbio.json, set $.env.IDB_REDIS_AUTH instead.
BROKER_URL = 'redis://10.13.45.208:6379/0'
CELERY_RESULT_BACKEND = 'redis://10.13.45.208:6379/0'

CELERYD_CONCURRENCY = 4
CELERY_TASK_SERIALIZER = 'json'
CELERY_RESULT_SERIALIZER = 'json'
CELERY_ACCEPT_CONTENT = ['json']
CELERY_TIMEZONE = 'America/New_York'
CELERY_ENABLE_UTC = True
CELERY_DISABLE_RATE_LIMITS = True
