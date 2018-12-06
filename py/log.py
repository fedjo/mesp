import logging
from systemd.journal import JournaldLogHandler

LOGGER_NAME = 'iotagent'

def setup_logger(logfile, loglevel=20): # INFO loglevel

    # Add a logger
    logger = logging.getLogger(LOGGER_NAME)
    # instantiate the JournaldLogHandler to hook into systemd
    journald_handler = JournaldLogHandler()
    # set a formatter to include the level name
    journald_handler.setFormatter(logging.Formatter(
            '[%(levelname)s] %(message)s'
    ))

    # instantiate the FileHandler
    file_handler = logging.FileHandler(logfile)
    file_handler.setFormatter(logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s'))

    # add the journald handler to the current logger
    logger.addHandler(journald_handler)
    logger.addHandler(file_handler)

    logger.setLevel(loglevel)
    logger.info("Started Agent Log")



def logger(name):
    return logging.getLogger(LOGGER_NAME + '.' + name)


