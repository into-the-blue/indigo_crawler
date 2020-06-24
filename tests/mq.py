from db import redis_conn
from rq import Queue

q = Queue(
    'test', connection=redis_conn, default_timeout='5h')
q.empty()

q2 = Queue(
    'test2', connection=redis_conn, default_timeout='5h')
q2.empty()
