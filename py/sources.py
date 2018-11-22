import threading
import time

import serial
from serial.serialposix import Serial
from confluent_kafka import Consumer, KafkaError
from confluent_kafka.avro import AvroConsumer
from confluent_kafka.avro.serializer import SerializerError


class GeneralSource(threading.Thread):
    def __init__(self, threadID, stype, name, q, queuelock, istr, logger):
        threading.Thread.__init__(self)
        self.threadID = threadID
        self.stype = stype
        self.name = name
        self.q = q
        self.lock = queuelock
        self.istr = istr
        self.logger = logger
        self.exitFlag = 0

    def read_data(self, threadName, q, istr):
        while not self.exitFlag:
            try:
                if isinstance(self.istr, (Serial, file)):
                    line = self.istr.readline()
                    # istr.reset_input_buffer()
                    try:
                        self.istr.flushInput()
                        self.istr.flushOutput()
                    except AttributeError as ae:
                        pass
                    # if line:
                    # if findWholeWord('UNIQUEDID'):

                    if line:
                        data = line.split()
                    else:
                        continue
                        # break
                if isinstance(self.istr, Consumer):
                    try:
                        msg = self.istr.poll(10)
                    except SerializerError as e:
                        self.logger.error("Message deserialization failed for {}: {}".format(msg, e))
                        break

                    if not msg:
                        continue
                    if msg.error():
                        if msg.error().code() == KafkaError._PARTITION_EOF:
                            continue
                        else:
                            self.logger.debug(msg.error())
                            break
                    data = msg.value()
                    self.logger.debug(data)

                self.lock.acquire()
                self.q.put(data[0])
                self.lock.release()
                # print("%s processing %s" % (threadName, data))
                time.sleep(1)
            except IOError as io:
                self.logger.debug(io)


    def run(self):
        self.logger.info("Starting %s %s" % (self.stype, self.name))
        self.read_data(self.name, self.q, self.istr)
        self.logger.info("Exiting %s %s" % (self.stype, self.name))


class SerialSource(GeneralSource):

    def __init__(self, threadID, name, q, queuelock, lconfig, logger):
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
                               istream, logger)


class FileSource(GeneralSource):

    def __init__(self, threadID, name, q, queuelock, lconfig, logger):
        istream = open(lconfig('FILE'), 'r')
        GeneralSource.__init__(self, threadID, 'File', name, q, queuelock,
                               istream, logger)


class KafkaSource(GeneralSource):

    def __init__(self, threadID, name, q, queuelock, lconfig, logger):
        if lconfig('SCHEMA'):
            c = AvroConsumer({
                'bootstrap.servers': lconfig('BROKER'),
                'group.id': lconfig('GROUP'),
                'schema.registry.url': lconfig('SCHEMA')
                })
            logger.debug("Avro Consumer")
        else:
            c = Consumer({
                'bootstrap.servers': lconfig('BROKER'),
                'group.id': lconfig('GROUP'),
                'auto.offset.reset': 'earliest'
                })
        c.subscribe(lconfig('TOPIC').split(','))
        istream = c
        GeneralSource.__init__(self, threadID, 'Kafka', name, q, queuelock,
                               istream, logger)
