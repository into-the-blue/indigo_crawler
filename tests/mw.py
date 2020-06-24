
from rq import Queue, Worker, Connection
from db import redis_conn


def start_worker(channels):
    with Connection(connection=redis_conn):
        worker = Worker(map(Queue, channels))
        worker.work()
