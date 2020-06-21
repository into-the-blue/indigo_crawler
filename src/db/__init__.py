# import os
# from pathlib import Path
# from dotenv import load_dotenv
# load_dotenv(dotenv_path=Path(os.path.dirname(__file__)).joinpath('../../dev.env'))

from .mongo import mongo,DB
from .redis import redis_conn
