from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.jobstores.mongodb import MongoDBJobStore
from db import DB
import os

db_ins = DB()

mongo_job_store = MongoDBJobStore(
    client=db_ins.conn, database=os.getenv('DB_DATABASE'), collection='schedules')

jobstores = {
    'mongo': mongo_job_store
}
sched = BackgroundScheduler(daemon=True, jobstores=jobstores)
