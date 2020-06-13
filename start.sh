celery -A taskq.server.celery worker -l info -P gevent -c 10 -n stream_worker1 --without-heartbeat
celery -A taskq.server.celery worker -l info -P gevent -c 10 -n stream_worker2 --without-heartbeat 
celery -A taskq.server.celery beat -l info

