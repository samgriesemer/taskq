import requests

from ..server import celery
from ..utils import db
from . import processing

@celery.on_after_finalize.connect
def schedule_tasks(sender, **kwargs):
  # retrieve tasks
  task_list = get_tasks()

  # schedule task
  for task in task_list:
    print(task['interval'])
    sender.add_periodic_task(
      task['interval'], 
      webget.s(task['url']),
      name=task['name'],
      link=callback_router.s(task['callback']),
      link_error=error_handler.s()
    )

@celery.task
def webget(url):
  try:
    res = requests.get(url, timeout=5)
    res = res.json()
  except:
    print('Error occurred in getting or processing JSON response.')
    print('Result: {}'.format(res.text))
  return res

@celery.task
def error_handler(err):
  print('ERROR: ' + err)
  # try:
  #   res = requests.get(url, timeout=5)
  #   res.raise_for_status()
  # except requests.exceptions.HTTPError as errh:
  #     print('HTTP error:', errh)
  # except requests.exceptions.ConnectionError as errc:
  #     print('Connection error:', errc)
  # except requests.exceptions.Timeout as errt:
  #     print('Timeout error:', errt)
  # except requests.exceptions.RequestException as err:
  #     print('Unknown request error:', err)
  
@celery.task
def callback_router(res, callback):
  processing.fmap[callback](res, db.connect())

def get_tasks():
  # get tasks from db
  conn = db.connect()
  tasks = db.getall(conn, 'SELECT * FROM tasks WHERE interval IS NOT NULL;')

  events = []
  for task in tasks:
    params = task['params'] if task['params'] else {}
    iter_params = task['iter']
    if iter_params:
      for itp in iter_params:
        events.append({
          'url': task['url'].format(**params, **itp),
          'interval': task['interval'],
          'callback': task['target'],
          'name': task['target']+'&'+str(itp)
        })
    else:
      events.append({
        'url': task['url'].format(**params),
        'interval': task['interval'],
        'callback': task['target'],
        'name': task['target']
      })
  return events
