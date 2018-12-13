import threading
import datetime
import sys
import csv
import busio
import adafruit_ina219
from board import SCL, SDA

from log import logger

LOGGER = logger(__name__)

class ConsumptionSource(threading.Thread):


    def __init__(self):
        threading.Thread.__init__(self)
        i2c_bus = busio.I2C(SCL, SDA)
        self.ina219 = adafruit_ina219.INA219(i2c_bus)
        self.exitFlag = 1
        self.retFlag = 0
        self.totalvoltage = 0.0
        self.totalcurrent = 0.0
        self.totalpower = 0.0
        self.counter = 0


    def run(self):
        while self.exitFlag:
            v = self.ina219.bus_voltage + self.ina219.shunt_voltage
            self.totalvoltage += v 
            c = self.ina219.current
            self.totalcurrent += c
            self.totalpower += (self.ina219.bus_voltage + self.ina219.shunt_voltage) * \
                    self.ina219.current
            self.counter += 1
        LOGGER.debug('Metrics taken')
        self.retFlag = 1

    def stop(self):
        LOGGER.debug('Exiting metrics')
        self.exitFlag = 0

    def get(self):
        while not self.retFlag:
            continue
        if self.counter != 0:
            v = (self.totalvoltage / self.counter)
            c = (self.totalcurrent / self.counter)
            p = (self.totalpower / self.counter)
            LOGGER.debug('Metrics {}, {}, {}'.format(v,c,p))
            return (v, c, p)
        else:
            return (self.load_voltage(), self.current(), self.power())

    def load_voltage(self):
        return (self.ina219.bus_voltage + self.ina219.shunt_voltage)

    def current(self):
        return self.ina219.current

    def power(self):
        return (self.load_voltage() * self.current())

    def get_metrics(self):
        return (self.load_voltage, self.current, self.power)
