import psycopg2 as pg
import psycopg2.pool
import psycopg2.extras

#conn_pool = pg.pool.ThreadedConnectionPool(1,20,user='dash',password='dashisfast901',database='dash',host='localhost',port=5432)

def connect(user='dash', password='dashisfast901', database='dash', host='localhost', port=5432):
  return pg.connect(host=host,database=database,user=user,password=password)

def connect_pool():
  return conn_pool.getconn()

def getall(conn, query, values=None):
  cur = conn.cursor(cursor_factory=pg.extras.RealDictCursor)
  cur.execute(query, values)
  return cur.fetchall()

def insert_many(conn, query, values):
  try:
    cur = conn.cursor()
    psycopg2.extras.execute_values(cur, query, values)
    conn.commit()
  except Exception as e:
    print('error in insert')
    with open('error.txt', 'a') as f:
      f.write('Rollback occurring, error {}'.format(e))
    conn.rollback()
  finally:
    cur.close()
