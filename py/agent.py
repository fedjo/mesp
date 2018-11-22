#!/usr/bin/python

import os
import platform
import threading
import logging
import sys
import ConfigParser
import re
import argparse
from systemd.journal import JournaldLogHandler
if sys.version_info[0] < 3:
    import Queue
else:
    import queue as Queue
from sources import SerialSource, FileSource, KafkaSource
from sink import OrionSink
from classification import TensorflowClassifier
from camera import Camera


def findWholeWord(w):
    return re.compile(r'\b({0})\b'.format(w), flags=re.IGNORECASE).search


def _setup_argparser():
    """Setup the command line arguments"""
    # Description
    parser = argparse.ArgumentParser(
        description="The agent.py application implements a non-blocking reader"
                    "on serial usb port or a Kafka server, a translator of the "
                    "data parsed and a writer to web ContextBroker (Orion)",
        usage="agent.py [options] input_stream service_location")

    # General Options
    gen_opts = parser.add_argument_group('General Options')
    gen_opts.add_argument("-f", "--config-file", metavar="[c]onfig-file",
                          help="Configuration file for agent.py",
                          type=str,
                          default="../conf/agent.ini")
    gen_opts.add_argument("--version",
                          help="print version information and exit",
                          action="store_true")
    gen_opts.add_argument("-q", "--quiet",
                          help="quiet mode, print no warnings and errors",
                          action="store_true")
    gen_opts.add_argument("-v", "--verbose",
                          help="verbose mode, print processing details",
                          action="store_true")
    gen_opts.add_argument("-d", "--debug",
                          help="debug mode, print debug information",
                          action="store_true")
    gen_opts.add_argument("-ll", "--log-level", metavar='[l]',
                          help="use level l for the logger"
                               "(fatal, error, warn, "
                               "info, debug, trace)",
                          type=str,
                          choices=['fatal', 'error', 'warn',
                                   'info', 'debug', 'trace'])
    gen_opts.add_argument("-lc", "--log-config", metavar='[f]',
                          help="use config file f for the logger",
                          type=str)

    # Process Options
    pr_opts = parser.add_argument_group('Process Options')
    pr_opts.add_argument("-rt", "--read-threads", metavar='[r]ead',
                          help="How many threads to run as readers",
                          type=int,
                          default=1)
    pr_opts.add_argument("-wt", "--write-threads", metavar='[w]rite',
                          help="How many threads to run as writers",
                          type=int,
                          default=1)
    pr_opts.add_argument("-usb", "--usb-port", metavar='[u]sbport',
                          help="Which usb port to read from",
                          type=int,
                          default=0)
    pr_opts.add_argument("-kf", "--kafka-url", metavar='[k]afka-url',
                          help="Where Kafka server is located",
                          type=str)
    pr_opts.add_argument("-kt", "--kafka-topic", metavar='[k]afka-topic',
                          help="Topic to consume from",
                          type=str)
    pr_opts.add_argument("-tf", "--tensorflow",
                         help="Classification using Tensorflow",
                         action="store_true")

    return parser.parse_args()


# Create two threads
if __name__ == "__main__":

    args = _setup_argparser()

    # Add a logger
    logger = logging.getLogger(__name__)
    # instantiate the JournaldLogHandler to hook into systemd
    journald_handler = JournaldLogHandler()
    # set a formatter to include the level name
    journald_handler.setFormatter(logging.Formatter(
            '[%(levelname)s] %(message)s'
    ))
    # add the journald handler to the current logger
    logger.addHandler(journald_handler)
    # optionally set the logging level
    if args.log_level:
        logger.setLevel(logging.INFO)
    else:
        logger.setLevel(logging.DEBUG)

    config =  ConfigParser.ConfigParser()
    config.read(args.config_file)
    SERIAL = lambda p: config.get('SERIAL', p)
    KAFKA = lambda p: config.get('KAFKA', p)
    ORION = lambda p: config.get('ORION', p)
    CLASSFCTN = lambda p: config.get('CLASSIFICATION', p)

    if args.tensorflow:
        tfclassify = TensorflowClassifier(2, CLASSFCTN('LABELS'),
                                          CLASSFCTN('FROZEN_GRAPH'))
        camera = Camera(CLASSFCTN('IMAGES_DIR'))

    _url = ORION('BROKER')
    if config.has_option('SERIAL', 'USB_PORT'):
        source = SerialSource
        sink = OrionSink
        lconfig = SERIAL
    elif config.has_option('SERIAL', 'FILE'):
        source = FileSource
        sink = OrionSink
        lconfig = SERIAL
    elif config.has_option('KAFKA', 'BROKER'):
        (os, ver, _) = platform.linux_distribution()
        if( os != 'debian' or float(ver) < 9.0):
            logger.error("OS version does not support Kafka consumer.")
            logger.error("Please upgrade to Debian version >=9 (Stretch).")
            sys.exit(-1)
        # Check for essential parameters
        if not(KAFKA('BROKER') or KAFKA('GROUP') or KAFKA('TOPIC')):
            logger.debug("Please specify all the parameters BROKER/GROUP/TOPIC for Kafka")
            sys.exit(-1)

        source = KafkaSource
        sink = OrionSink
        lconfig = KAFKA
        #client = KafkaClient(hosts=args.kafka_url)
    else:
        logger.debug("Configuration file does not provide any input stream")
        logger.debug("Please provide one of the following inputs:")
        logger.debug("SERIAL/KAFKA")
        sys.exit(-1)


    readerThreadList = []
    for i in range(args.read_threads):
        readerThreadList.append("Reader-"+ str(i+1))

    writerThreadList = []
    for i in range(args.write_threads):
        writerThreadList.append("Writer-"+ str(i+1))

    queueLock = threading.Lock()
    workQueue = Queue.Queue()
    threads = []
    threadID = 1

    try:
        logger.info("Reading schema of data...")
        while True:
            # schema = istream.readline();
            schema = SERIAL('SCHEMA')
            if not schema:
                continue
            else:
                logger.info(schema)
                break
        logger.debug("Starting receiving data...")
    except Exception as e:
        logger.debug("Could not read schema!")
        sys.exit(-1)

    # Create new threads
    for tName in readerThreadList:
        thread = source(threadID, tName, workQueue, queueLock, lconfig, logger)
        thread.start()
        threads.append(thread)
        threadID += 1

    for tName in writerThreadList:
        thread = sink(threadID, tName, workQueue, queueLock, _url, schema, logger)
        thread.start()
        threads.append(thread)
        threadID += 1

    # Wait for queue to empty
    #while not exitFlag:
    #    pass

    # Wait for threads to complete
    for t in threads:
        t.join()
    logger.info("Exiting Main thread!")
