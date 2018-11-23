import threading
import datetime
import time
import busio
import adafruit_ina219
from board import SCL, SDA


class ConsumptionSource(threading.Thread):
    def __init__(self, metrics_filepath):
        i2c_bus = busio.I2C(SCL, SDA)
        ina219 = adafruit_ina219.INA219(i2c_bus)
        self.load_voltage = ina219.bus_voltage + ina219.shunt_voltage
        self.current = ina219.current
        self.power = self.load_voltage * self.current
        self.filepath = metrics_filepath

    def get_metrics(self):
        return (self.load_voltage, self.current, self.power)

    def run(self):
        with open(self.filepath, 'w+') as f:
            while True:
                f.write("Writing")
                f.write("{}:\tVoltage: {} V\n\tCurrent: {} mA\n\tPower: {} mW".
                        format(str(datetime.datetime.now()), self.load_voltage,
                               self.current, self.power))
                f.flush()
                time.sleep(2)



if __name__ == "__main__":

    # Code to write metrics to a file
    consumption = ConsumptionSource('/home/pi/Desktop/mdpi-metrics.txt')
    consumption.run()
