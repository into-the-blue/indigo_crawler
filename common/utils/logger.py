import sys
import logging
from datetime import datetime, timezone, timedelta,date


def get_logger():
    logger = logging.getLogger("INDIGO")

    handler1 = logging.StreamHandler()
    handler2 = logging.handlers.TimedRotatingFileHandler(
        filename="../logs/{}.log".format(date.today().isoformat()), when="D", backupCount=10, interval=1)
    
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
