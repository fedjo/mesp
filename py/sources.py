import threading

import serial
from serial.serialposix import Serial
from confluent_kafka import Consumer, KafkaError
from confluent_kafka.avro import AvroConsumer
from confluent_kafka.avro.serializer import SerializerError

from log import logger


LOGGER = logger(__name__)


class GeneralSource(threading.Thread):
    def __init__(self, threadID, stype, name, clf,
                 scq, sclock, skq, sklock, istr):
        threading.Thread.__init__(self)
        self.threadID = threadID
        self.stype = stype
        self.name = name
        self.skq = skq
        self.sklock = sklock
        self.istr = istr
        self.exitFlag = 0
        self.clf = clf

    def read_data(self):
        while not self.exitFlag:
            try:

                rawdata = dict()
                if self.clf:
                    self.sclock.aquire()
                    rawdata['score'] = self.scq.get()
                    self.sclock.release()

                if isinstance(self.istr, (Serial, file)):
                    try:
                        self.istr.reset_input_buffer()
                        self.istr.flushInput()
                        self.istr.flushOutput()
                    except AttributeError as ae:
                        pass
                    # if line:
                    # if findWholeWord('UNIQUEDID'):

                    line = self.istr.readline()
                    if (not line or len(line) <= 20):
                        continue

                    d = line.split()
                    data = d[0]

                if isinstance(self.istr, Consumer):
                    try:
                        msg = self.istr.poll(10)
                    except SerializerError as e:
                        LOGGER.error("Cannot deserialise message {}: {}"
                                     .format(msg, e))
                        break

                    if not msg:
                        continue
                    if msg.error():
                        if msg.error().code() == KafkaError._PARTITION_EOF:
                            continue
                        else:
                            LOGGER.debug(msg.error())
                            break

                    data = msg.value()
                    LOGGER.debug(data)

                # Merge classification result with data
                rawdata['raw'] = data
                LOGGER.debug("Adding to queue rawdata: {}".format(rawdata))
                self.sklock.acquire()
                self.skq.put(rawdata)
                self.sklock.release()
            except IOError as io:
                LOGGER.debug(io)

    def run(self):
        LOGGER.info("Starting %s %s" % (self.stype, self.name))
        self.read_data()
        LOGGER.info("Exiting %s %s" % (self.stype, self.name))


class SerialSource(GeneralSource):

    def __init__(self, threadID, name, clf, scq, sclock, skq, sklock, lconfig):
        istream = serial.Serial(
            '/dev/ttyUSB' + lconfig('USB_PORT'),
            baudrate=38400,
            timeout=2,
            bytesize=serial.EIGHTBITS,
            parity=serial.PARITY_NONE,
            stopbits=serial.STOPBITS_ONE,
            xonxoff=False
        )
        GeneralSource.__init__(self, threadID, 'Serial', name, clf,
                               scq, sclock, skq, sklock, istream)


class FileSource(GeneralSource):

    def __init__(self, threadID, name, clf, scq, sclock, skq, sklock, lconfig):
        istream = open(lconfig('FILE'), 'r')
        GeneralSource.__init__(self, threadID, 'File', name, clf,
                               scq, sclock, skq, sklock, istream)


class KafkaSource(GeneralSource):

    def __init__(self, threadID, name, clf, scq, sclock, skq, sklock, lconfig):
        if lconfig('SCHEMA'):
            c = AvroConsumer({
                'bootstrap.servers': lconfig('BROKER'),
                'group.id': lconfig('GROUP'),
                'schema.registry.url': lconfig('SCHEMA')
            })
            LOGGER.debug("Avro Consumer")
        else:
            c = Consumer({
                'bootstrap.servers': lconfig('BROKER'),
                'group.id': lconfig('GROUP'),
                'auto.offset.reset': 'earliest'
            })
        c.subscribe(lconfig('TOPIC').split(','))
        istream = c
        GeneralSource.__init__(self, threadID, 'Kafka', name, clf,
                               scq, sclock, skq, sklock, istream)
