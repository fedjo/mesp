#!/home/lebowski/.virtualenvs/iot2edge/bin/python

import os
import threading
import requests
import datetime
import logging
import time
import sys
import ConfigParser
import serial
import re
import argparse
from systemd.journal import JournaldLogHandler
if sys.version_info[0] < 3:
    import Queue
else:
    import queue as Queue

from pykafka.simpleconsumer import SimpleConsumer

from confluent_kafka import Consumer, KafkaError
from confluent_kafka.avro import AvroConsumer
from confluent_kafka.avro.serializer import SerializerError

from serial.serialposix import Serial

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
logger.setLevel(logging.DEBUG)

exitFlag = 0


measurments = []
cross_ref_unique_ids = []

experimental_results_list = []


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
                          default="agent.ini")
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

    return parser.parse_args()


class KafkaReader(threading.Thread):
    def __init__(self, threadID, name, q, istr):
        threading.Thread.__init__(self)
        self.threadID = threadID
        self.name = name
        self.q = q
        self.istr = istr

    def run(self):
        logger.info("Starting Kafka " + self.name)
        read_data(self.name, self.q, self.istr)
        logger.info("Exiting Kafka " + self.name)


class SerialReader(threading.Thread):
    def __init__(self, threadID, name, q, istr):
        threading.Thread.__init__(self)
        self.threadID = threadID
        self.name = name
        self.q = q
        self.istr = istr

    def run(self):
        logger.info("Starting Serial " + self.name)
        read_data(self.name, self.q, self.istr)
        logger.info("Exiting Serial " + self.name)


class OrionWriter(threading.Thread):
    def __init__(self, threadID, name, q, schema):
        threading.Thread.__init__(self)
        self.threadID = threadID
        self.name = name
        self.q = q
        self.schema = schema

    def run(self):
        logger.info("Starting Orion " + self.name)
        process_data(self.name, self.q)
        logger.info("Exiting Orion " + self.name)


def read_data(threadName, q, istr):
    while not exitFlag:
        try:
            if isinstance(istr, (Serial, file)):
                line = istr.readline()
                # istr.reset_input_buffer()
                istr.flushInput()
                istr.flushOutput()
                # if line:
                # if findWholeWord('UNIQUEDID'):

                if line:
                    data = line.split()
                else:
                    continue
                    # break
            if isinstance(istr, Consumer):
                msg =istr.poll(1.0)
                if not msg:
                    continue
                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        continue
                    else:
                        logger.debug(msg.error())
                        break
                data = msg.value().decode('utf-8')
                logger.debug(data)

            queueLock.acquire()
            #q.put(data[0])
            queueLock.release()
            # print("%s processing %s" % (threadName, data))
            time.sleep(1)
        except IOError:
            logger.debug('Cannot open file: sensed_data')
            logger.debug('Make sure that negative_list file exists in the same folder as ??.py')


def process_data(threadName, q):
    while not exitFlag:
        if not q.empty():
            queueLock.acquire()
            data = q.get()
            queueLock.release()
            # print("%s Got data %s" % (threadName, data))
            posttoorion(data, schema)
        else:
            continue


def calltoopenweathermap():
    logger.debug("Goodbye, World!")
    weather_url = 'http://api.openweathermap.org/data/2.5/forecast?lat=38.303860&lon=23.730180&cnt=5&appid=3a87a263c645ea5eb18ad7417be4cb0d'
    logger.debug(weather_url)
    response = requests.post(weather_url)
    logger.debug(response.json())


def getfromorion_id(id):
    logger.debug("get from Orion!")
    url = _url + '/v2/entities/' + id
    headers = {'Accept': 'application/json', 'X-Auth-Token': 'QGIrJsK6sSyKfvZvnsza6DlgjSUa8t'}
    # print url
    response = requests.get(url, headers=headers)
    return response.json()


def getfromorion_all():
    logger.debug("get from Orion!")
    # payload = {'limit': '500'}
    url = _url + '/v2/entities?limit=500'
    headers = {'Accept': 'application/json', 'X-Auth-Token': 'QGIrJsK6sSyKfvZvnsza6DlgjSUa8t' }
    # print url
    response = requests.get(url, headers=headers)
    # print response.json()
    return response.json()


def load_measurments(data_stream):
    sensed_data_list = []
    try:
        with open(data_stream) as f:
            for line in f:
                key = line.split()
                sensed_data_list.append(key[0])
    except IOError:
        logger.debug('Cannot open file: sensed_data')
        logger.debug('Make sure that negative_list file exists in the same folder as ??.py')
    return sensed_data_list


def posttoorion(snapshot_raw, schema):
    logger.info("Parsing data...")
    logger.info(snapshot_raw)

    batches = schema.split(';')[:-1]
    data = snapshot_raw.split(";")[:-1]

    if len(batches) != len(data):
        logger.debug("Schema and data format are not the same!")
        raise Exception("Schema and data format are not the same!")

    experimental_results = {}
    ts1_received = time.time()
    experimental_results["ts1_received"] = ts1_received

    snapshot = {}

    for B, d in zip(batches, data):
        snapshot[B.lower()] = d

    logger.info("Post to Orion!")
    logger.info(snapshot)
    # print("TIME")
    pts = datetime.datetime.now().strftime('%s')
    # print(pts)

    json = translate(snapshot, pts)
    ts2 = time.time()
    logger.debug(json)
    # print "posting"

    translation_time = ts2-ts1_received
    volume = sys.getsizeof(json)
    logger.debug("translation time")
    logger.info("entity id, volume, translation_time")
    logger.info(str(pts), volume, translation_time)

    if _url:
        url = _url + '/v2/entities'
        headers = {'Accept': 'application/json', 'X-Auth-Token': 'QGIrJsK6sSyKfvZvnsza6DlgjSUa8t'}
        # print url
        response = requests.post(url, json=json)

        logger.debug("response")
        logger.debug(response.text)

    cross_ref_unique_ids.append(str(pts))
    logger.debug("list of ids translated and send:")
    logger.debug(str(pts))


def translate(snapshot_dict, timestamp):

    json = {

        "id": str(timestamp),
        "type": "Sensor123",
        "translation_timestamp": {
            "value": str(timestamp),
            "type": "time"
        }
    }

    value = dict()
    types = {
            'nodeid': 'id',
            'gps_location': 'GPS',
            'humidity': 'Number',
            'flame': 'Number',
            'temp-air': 'Number',
            'gas': 'Number',
            'temp-soil': 'Number',
            'uniqueid': 'time',
            'epoch': 'time'
    }
    for k,v in snapshot_dict.iteritems():
        if '#' in k:
            k = k[:-2]
        if 'gps' in k:
            k = 'gps_location'
        value[k] = {
            "value": v,
            "type": types[k]
        }
        json.update(value)
    logger.debug(json)
    return json


def post_all(measurments_list):
    logger.debug("reading measurment")
    for measurment_one in measurments_list:
        time.sleep(1)
        logger.debug(measurment_one)
        logger.debug("posting")
        posttoorion(measurment_one)
        logger.debug("done")

    logger.debug("finished")


def retrieve_id():
    for id in cross_ref_unique_ids:
        logger.debug("retrieving : " + str(id))
        res = getfromorion_id(id)
        logger.debug(res['id'])
        if res['id'] == id:
            logger.debug("found")
        else:
            logger.debug("not found")


# Create two threads
if __name__ == "__main__":

    args = _setup_argparser()

    config =  ConfigParser.ConfigParser()
    config.read(args.config_file)
    SERIAL = lambda p: config.get('SERIAL', p)
    KAFKA = lambda p: config.get('KAFKA', p)
    ORION = lambda p: config.get('ORION', p)

    _url = ORION('BROKER')
    if config.has_option('SERIAL', 'USB_PORT'):
        readerClass = SerialReader
        writerClass = OrionWriter
        istream = serial.Serial(
                '/dev/ttyUSB'+SERIAL('USB_PORT'),
                baudrate=38400,
                timeout=2,
                bytesize=serial.EIGHTBITS,
                parity=serial.PARITY_NONE,
                stopbits=serial.STOPBITS_ONE,
                xonxoff=False
        )
    elif config.has_option('SERIAL', 'FILE'):
        readerClass = SerialReader
        writerClass = OrionWriter
        istream = open(SERIAL('FILE'), 'r')
    elif config.has_option('KAFKA', 'BROKER'):
        # Check for essential parameters
        if not(KAFKA('BROKER') or KAFKA('GROUP') or KAFKA('TOPIC')):
            logger.debug("Please specify all the parameters BROKER/GROUP/TOPIC for Kafka")
            sys.exit(-1)

        readerClass = KafkaReader
        writerClass = OrionWriter
        #client = KafkaClient(hosts=args.kafka_url)
        if config.has_option('KAFKA', 'SCHEMA'):
            c = AvroConsumer({
                'bootstrap.servers': KAFKA('BROKER'),
                'group.id': KAFKA('GROUP'),
                'schema.registry.url': KAFKA('SCHEMA')
                })
        else:
            c = Consumer({
                'bootstrap.servers': KAFKA('BROKER'),
                'group.id': KAFKA('GROUP'),
                'auto.offset.reset': 'earliest'
                })
        c.subscribe([KAFKA('TOPIC')])
        istream = c
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
        thread = readerClass(threadID, tName, workQueue, istream)
        thread.start()
        threads.append(thread)
        threadID += 1

    for tName in writerThreadList:
        thread = writerClass(threadID, tName, workQueue, schema)
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

    # load measurments from file (1sec interval)
    #values = load_measurments(sys.argv[2])
    #
    #post_all(values)
    # print("cross_ref_unique_ids")
    #print cross_ref_unique_ids


    # retrieve_id()

    # all = getfromorion_all()
    # print len(all)