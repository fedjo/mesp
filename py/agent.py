#!/usr/bin/python

import os
import platform
import threading
import sys
import time
import re
import argparse
if sys.version_info[0] < 3:
    import Queue
    import ConfigParser
else:
    import queue as Queue
    import configparser as ConfigParser
from log import setup_logger, logger
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
    gen_opts.add_argument("-d", "--debug",
                          help="debug mode, print debug information",
                          action="store_true")

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
    pr_opts.add_argument("-tf", "--tensorflow",
                         help="Classification using Tensorflow",
                         action="store_true")

    return parser.parse_args()


# Create two threads
if __name__ == "__main__":

    args = _setup_argparser()

    config =  ConfigParser.ConfigParser()
    config.read(args.config_file)
    SERIAL = lambda p: config.get('SERIAL', p)
    KAFKA = lambda p: config.get('KAFKA', p)
    ORION = lambda p: config.get('ORION', p)
    CLASSFCTN = lambda p: config.get('CLASSIFICATION', p)
    LOG = lambda p: config.get('LOG', p)

    # set the logging level
    if args.debug:
        setup_logger(logfile=LOG('LOGFILE'), loglevel=10)
    else:
        setup_logger(logfile=LOG('LOGFILE'))
    LOGGER = logger(__name__)

    if args.tensorflow:
        camera = Camera(CLASSFCTN('IMAGES_DIR'))
        tfclassify = TensorflowClassifier(CLASSFCTN('LABELS'),
                                          CLASSFCTN('FROZEN_GRAPH'))

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
            LOGGER.error("OS version does not support Kafka consumer.")
            LOGGER.error("Please upgrade to Debian version >=9 (Stretch).")
            sys.exit(-1)
        # Check for essential parameters
        if not(KAFKA('BROKER') or KAFKA('GROUP') or KAFKA('TOPIC')):
            LOGGER.debug("Please specify all the parameters BROKER/GROUP/TOPIC for Kafka")
            sys.exit(-1)

        source = KafkaSource
        sink = OrionSink
        lconfig = KAFKA
        #client = KafkaClient(hosts=args.kafka_url)
    else:
        LOGGER.debug("Configuration file does not provide any input stream")
        LOGGER.debug("Please provide one of the following inputs:")
        LOGGER.debug("SERIAL/KAFKA")
        sys.exit(-1)

    queueLock = threading.Lock()
    workQueue = Queue.Queue()
    threadID = 1

    LOGGER.info("Reading schema of data...")
    schema = SERIAL('SCHEMA')
    if not schema:
        LOGGER.debug("Could not read schema!")
        sys.exit(-1)
    else:
        LOGGER.info(schema)
    LOGGER.info("Starting receiving data...")

    # Create new threads
    readerThreadList = []
    for i in range(args.read_threads):
        tname = "Reader-%d" % i
        thread = source(threadID, tname, workQueue, queueLock, lconfig)
        # thread.start()
        readerThreadList.append(thread)
        threadID += 1

    writerThreadList = []
    for i in range(args.write_threads):
        tname = "Writer-%d" % i
        thread = sink(threadID, tname, workQueue, queueLock, _url, schema, LOG('METRICSFILE'))
        # thread.start()
        writerThreadList.append(thread)
        threadID += 1

    while 1:
        if(camera and tfclassify):
            top_k = tfclassify.classify(camera.capture())
            if(len(readerThreadList) > 0 and
               len(writerThreadList) > 0):
                readerThreadList[0]._read_data()
                body_sizes = writerThreadList[0]._process_data(top_k)
                LOGGER.info(body_sizes)
                time.sleep(45)


    # Wait for threads to complete
    for t in threads:
        t.join()
    LOGGER.info("Exiting Main thread!")
