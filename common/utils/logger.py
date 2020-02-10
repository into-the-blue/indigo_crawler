import sys
import logging
from logging import handlers
from datetime import datetime, timezone, timedelta, date
from pathlib import Path
LOG_FILE_DIR = str((Path(__file__)/'..'/'..'/'logs').resolve())


def get_logger():
    logger = logging.getLogger("INDIGO")

    handler1 = logging.StreamHandler()
    handler2 = handlers.TimedRotatingFileHandler(
        filename=LOG_FILE_DIR+"/{}.log".format(date.today().isoformat()), when="D", backupCount=10, interval=1)
    logger.setLevel(logging.INFO)
    handler1.setLevel(logging.INFO)
    handler2.setLevel(logging.ERROR)

    formatter = logging.Formatter(
        "%(asctime)s %(name)s %(levelname)s: %(message)s")
    handler1.setFormatter(formatter)
    handler2.setFormatter(formatter)

    logger.addHandler(handler1)
    logger.addHandler(handler2)
    return logger


logger = get_logger()
