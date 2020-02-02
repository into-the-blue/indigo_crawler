import sys
import logging
from datetime import datetime, timezone, timedelta

logging.basicConfig(stream=sys.stdout, level=logging.INFO,
                    datefmt='%Y-%m-%d %H:%M:%S')
logger = logging.getLogger('INDIGO')