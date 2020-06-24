import redis
import os

REDIS_HOST = os.getenv('REDIS_HOST')
REDIS_PASSWORD = os.getenv('REDIS_PASSWORD')
redis_conn = redis.Redis(
    host=REDIS_HOST,
    port=6379,
    password=REDIS_PASSWORD
)
