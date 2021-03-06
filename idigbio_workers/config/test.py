BROKER_URL = 'redis://idb-redis-celery-beta.acis.ufl.edu:6379/0'
CELERY_RESULT_BACKEND = 'redis://idb-redis-celery-beta.acis.ufl.edu:6379/0'

CELERYD_CONCURRENCY = 4
CELERY_TASK_SERIALIZER = 'json'
CELERY_RESULT_SERIALIZER = 'json'
CELERY_ACCEPT_CONTENT = ['json']
CELERY_TIMEZONE = 'America/New_York'
CELERY_ENABLE_UTC = True
CELERY_DISABLE_RATE_LIMITS = True
