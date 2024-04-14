import logging
from logging.handlers import TimedRotatingFileHandler
from time import sleep


def set_logger(engine: str):
    handler = TimedRotatingFileHandler(f'v1/logs/{engine}.log',
                                       when='midnight',
                                       utc=True,
                                       backupCount=5)
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s : %(levelname)s : %(threadName)s : %(funcName)s : %(module)s : %(message)s',
        handlers=[handler]
    )


def tail(path):
    with open(path, 'r') as fh:
        # Go to the end of the file
        fh.seek(0, 2)
        while True:
            line = fh.readline()
            if not line:
                # Sleep briefly
                sleep(0.1)
                continue
            print(line.rstrip())
