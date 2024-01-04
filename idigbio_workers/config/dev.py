#NOTE: password supplied in broker_url is ignored;
# in idigbio.json, set $.env.IDB_REDIS_AUTH instead.
broker_url = 'redis://127.0.0.1:6379/1'
broker_transport_options = {
    'fanout_prefix': True,
    'fanout_patterns': True,
    'visibility_timeout': 43200
}
result_backend = broker_url

worker_concurrency = 4
worker_prefetch_multiplier = 1


timezone = 'America/New_York'

worker_disable_rate_limits = True
