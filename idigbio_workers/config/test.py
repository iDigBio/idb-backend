# idigbio_workers/config/test.py

broker_url = "redis://10.13.44.11:6379/1"
result_backend = "redis://10.13.44.11:6379/1"
accept_content = ["json"]
task_serializer = "json"
result_serializer = "json"
result_accept_content = ["json"]
timezone = "America/New_York"
enable_utc = True
worker_concurrency = 4
worker_disable_rate_limits = True
