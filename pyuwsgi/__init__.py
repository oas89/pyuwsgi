import logging

log_format = '%(asctime)s - %(message)s'
date_format = '%Y-%m-%d %H:%M:%S'

logging.basicConfig(level=logging.DEBUG, format=log_format, datefmt=date_format)
logger = logging.getLogger(__name__)
