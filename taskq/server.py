from celery import Celery

# construct celery app
celery = Celery()
celery.config_from_object('taskq.celeryconfig')

# add directory containing local tasks.py module
celery.autodiscover_tasks(['taskq.web',])
