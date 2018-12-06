import threading
import time
import datetime

import serial
from serial.serialposix import Serial
from confluent_kafka import Consumer, KafkaError
from confluent_kafka.avro import AvroConsumer
from confluent_kafka.avro.serializer import SerializerError

from log import logger

LOGGER = logger(__name__)

class GeneralSource(threading.Thread):
    def __init__(self, threadID, stype, name, q, queuelock, istr):
        threading.Thread.__init__(self)
        self.threadID = threadID
        self.stype = stype
        self.name = name
        self.q = q
        self.lock = queuelock
        self.istr = istr
        self.exitFlag = 0

    def _read_data(self):
        self.read_data(self.name, self.q, self.istr)

    def read_data(self, threadName, q, istr):
        while not self.exitFlag:
            try:
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
                    if not line:
                        continue

                    data = line.split()

                if isinstance(self.istr, Consumer):
                    try:
                        msg = self.istr.poll(10)
                    except SerializerError as e:
                        LOGGER.error("Message deserialization failed for {}: {}".format(msg, e))
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

                self.lock.acquire()
                self.q.put(data[0])
                self.lock.release()
                break
            except IOError as io:
                LOGGER.debug(io)


    def run(self):
        LOGGER.info("Starting %s %s" % (self.stype, self.name))
        self.read_data(self.name, self.q, self.istr)
        LOGGER.info("Exiting %s %s" % (self.stype, self.name))


class SerialSource(GeneralSource):

    def __init__(self, threadID, name, q, queuelock, lconfig):
        istream = serial.Serial(
                '/dev/ttyUSB' + lconfig('USB_PORT'),
                baudrate=38400,
                timeout=2,
                bytesize=serial.EIGHTBITS,
                parity=serial.PARITY_NONE,
                stopbits=serial.STOPBITS_ONE,
                xonxoff=False
        )
        GeneralSource.__init__(self, threadID, 'Serial', name, q, queuelock,
                               istream)


class FileSource(GeneralSource):

    def __init__(self, threadID, name, q, queuelock, lconfig):
        istream = open(lconfig('FILE'), 'r')
        GeneralSource.__init__(self, threadID, 'File', name, q, queuelock,
                               istream)


class KafkaSource(GeneralSource):

    def __init__(self, threadID, name, q, queuelock, lconfig):
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
        GeneralSource.__init__(self, threadID, 'Kafka', name, q, queuelock,
                               istream)
