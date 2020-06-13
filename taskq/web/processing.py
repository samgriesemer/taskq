from datetime import datetime
import pytz
import sys, inspect
from psycopg2.extras import Json

from ..utils import db
from ..server import celery

# time settings
est = pytz.timezone('US/Eastern')

@celery.task
def news(res, conn):
  values = []
  article_list = res['articles']
  for article in article_list:
    values.append([
      article['title'],
      article['author'],
      article['source']['name'],
      article['description'],
      article['url'],
      article['publishedAt'],
      'top-headlines',
      '',
      datetime.utcnow().replace(microsecond=0).isoformat(),
    ])

  query = '''INSERT INTO news VALUES %s
             ON CONFLICT (title, source) 
             DO UPDATE SET query_time = EXCLUDED.query_time'''

  # attempt to commit changes
  db.insert_many(conn, query, values)

@celery.task
def weather(res, conn):
  data = res['currently']
  values = []
  values.append([
    datetime.utcnow().replace(microsecond=0).isoformat(),
    data['summary'],
    data['temperature'],
    data['apparentTemperature'],
    data['precipType'] if 'precipType' in data else None,
    data['precipProbability'],
    data['humidity'],
    data['pressure'],
    data['windSpeed'],
  ])

  query = 'INSERT INTO weather VALUES %s'

  # attempt to commit changes
  db.insert_many(conn, query, values)

@celery.task
def crypto(res, conn):
  data = res['RAW']
  values = []
  for key in data.keys():
    asset = data[key]['USD']
    values.append([
      datetime.utcnow().replace(microsecond=0).isoformat(),
      asset['FROMSYMBOL'],
      asset['PRICE'],
      asset['MKTCAP'],
      asset['TOTALVOLUME24HTO'],
      asset['CHANGE24HOUR'],
      asset['CHANGEPCT24HOUR'],
    ])

  query = 'INSERT INTO crypto VALUES %s'

  # attempt to commit changes
  db.insert_many(conn, query, values)

@celery.task
def stocks_depracated(res, conn):
  values = []
  for symbol in res.keys():
    chart = res[symbol]['chart']
    for item in chart:
      dstr = item['date']+item['minute']
      pdt = datetime.strptime(dstr,'%Y%m%d%H:%M')
      pdt = est.localize(pdt).astimezone(pytz.utc)
      values.append([
        symbol.lower(),
        item['open'] if 'open' in item else None,
        item['close'] if 'close' in item else None,
        item['high'] if 'high' in item else None,
        item['low'] if 'low' in item else None,
        item['volume'],
        pdt,
      ])

  query = '''INSERT INTO stocks VALUES %s
             ON CONFLICT DO NOTHING'''

  # attempt to commit changes
  db.insert_many(conn, query, values)
  
@celery.task
def stocks_realtime(res, conn):
  '''
  {
    "symbol": "SNAP",
    "bidSize": 200,
    "bidPrice": 110.94,
    "askSize": 100,
    "askPrice": 111.82,
    "volume": 177265,
    "lastSalePrice": 111.76,
    "lastSaleSize": 5,
    "lastSaleTime": 1480446905681,
    "lastUpdated": 1480446910557,
    "sector": "softwareservices",
    "securityType": "commonstock"
  }
  '''
  
  values = []
  for stock in res:
    values.append([
      stock['symbol'],
      datetime.utcnow().replace(microsecond=0).isoformat(),
      stock['bidSize'],
      stock['bidPrice'],
      stock['askSize'],
      stock['askPrice'],
      stock['volume'],
      stock['lastSalePrice'],
      stock['lastSaleSize'],
      datetime.utcfromtimestamp(stock['lastSaleTime']/1000).replace(microsecond=0).isoformat(),
      datetime.utcfromtimestamp(stock['lastUpdated']/1000).replace(microsecond=0).isoformat(),
      stock['sector'],
      stock['securityType']
    ])
  
  query = '''INSERT INTO stocks_realtime VALUES %s
             ON CONFLICT DO NOTHING'''

  # attempt to commit changes
  db.insert_many(conn, query, values)

@celery.task
def stocks_DEEP(res, conn):
  '''
  Response example:

  {
    "symbol": "SNAP",
    "marketPercent": 0.00837,
    "volume": 359425,
    "lastSalePrice": 22.975,
    "lastSaleSize": 100,
    "lastSaleTime": 1494446394043,
    "lastUpdated": 1494446715171,
    "bids": [
        {
          "price": 19.13,
          "size": 650,
          "timestamp": 1494446715171
        }
    ],
    "asks": [
        {
          "price": 19.15,
          "size": 891,
          "timestamp": 1494446717238
        }
    ]
  }
  '''
  print('im committing!!!!!!!!')
  values = []
  values.append([
    res['symbol'],
    datetime.utcnow().replace(microsecond=0).isoformat(),
    res['marketPercent'],
    res['volume'],
    res['lastSalePrice'],
    res['lastSaleSize'],
    datetime.utcfromtimestamp(res['lastSaleTime']/1000).replace(microsecond=0).isoformat(),
    datetime.utcfromtimestamp(res['lastUpdated']/1000).replace(microsecond=0).isoformat(),
    Json(res['bids']),
    Json(res['asks'])
  ])
  
  print('Values: {}'.format(values))

  query = '''INSERT INTO stocks_realtime VALUES %s
             ON CONFLICT DO NOTHING'''

  # attempt to commit changes
  db.insert_many(conn, query, values)

@celery.task
def stocks_intraday(res, conn):
  values = []
  print(res)
  symbol = res['Meta Data']['2. Symbol']
  series = res['Time Series (1min)']

  for (dstr, data) in series.items():
    pdt = datetime.datetime.strptime(dstr,'%Y-%m-%d %H:%M:%S')
    pdt = est.localize(pdt).astimezone(pytz.utc)

    values.append([
      symbol.upper(),
      pdt,
      data['1. open'],
      data['2. high'],
      data['3. low'],
      data['4. close'],
      data['5. volume'],
    ])

  query = '''INSERT INTO stocks VALUES %s
             ON CONFLICT DO NOTHING'''

  # attempt to commit changes
  db.insert_many(conn, query, values)

# generate name-function map
#fmap = dict(inspect.getmembers(sys.modules[__name__], inspect.isfunction))

fmap = {'news':news,
        'weather':weather,
	      'crypto':crypto,
	      'stocks_realtime':stocks_realtime}
