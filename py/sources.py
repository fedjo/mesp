import threading
import time
import datetime

import serial
import busio
import adafruit_ina219
from board import SCL, SDA
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

    def _read_data(self):
        self.read_data(self.name, self.q, self.istr)

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

                    if not line:
                        continue

                    data = line.split()

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
                break
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


class ConsumptionSource(threading):
    def __init__(self, metrics_file)
        i2c_bus = busio.I2C(SCL, SDA)
        ina219 = adafruit_ina219.INA219(i2c_bus)
        self.load_voltage = ina219.bus_voltage + ina219.shunt_voltage
        self.current = ina219.current
        self.power = self.load_voltage * self.current
        self.filepath = metrics_filepath

    def get_metrics(self):
        return (self.load_voltage, self.current, self.power)

    def run(self):
        with open(self.filepath, 'wa+') as f:
            while True:
                f.write("{}:\tVoltage: {} V\n\tCurrent: {} mA\n\tPower: {} mW".
                        format(str(datetime.datetime.now()), self.load_voltage,
                               self.current, self.power))
                time.sleep(2)

