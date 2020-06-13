broker_url = 'amqp://stream:streamisflow549@localhost/stream'

task_serializer = 'json'
result_serializer = 'json'
accept_content = ['json']
enable_utc = True

broker_heartbeat = 0
#broker_pool_limit = 1 # Will decrease connection usage
broker_connection_timeout = 30 # May require a long timeout due to Linux DNS timeouts etc
result_backend = None # AMQP is not recommended as result backend as it creates thousands of queues
event_queue_expires = 60 # Will delete all celeryev. queues without consumers after 1 minute.
worker_prefetch_multiplier = 1 # Disable prefetching, it's causes problems and doesn't help performance
worker_concurrency = 8 # If you tasks are CPU bound, then limit to the number of cores, otherwise increase substainally
result_expires = 3600
#result_persistent = False # disabled by default
task_ignore_result = True
