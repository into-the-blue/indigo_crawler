from db import redis_conn
from rq import Queue

q_url_crawler = Queue(
    'url_crawler', connection=redis_conn, default_timeout='5h')
q_detail_crawler = Queue(
    'detail_crawler', connection=redis_conn, default_timeout='10m')
q_validator = Queue('validator', connection=redis_conn, default_timeout='5m')

CHANNELS = [
    'url_crawler',
    'detail_crawler',
    'validator'
]
