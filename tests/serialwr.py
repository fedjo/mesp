#!/usr/bin/env python

import time
import serial


ser = serial.Serial(
    port='/dev/ttyAMA0',
    baudrate = 9600,
    parity=serial.PARITY_NONE,
    stopbits=serial.STOPBITS_ONE,
    bytesize=serial.EIGHTBITS,
    timeoule 1:
                   ser.write('Write counter: %d \n'%(counter))
                                  time.sleep(1)
                                                 counter += 1t=1
)
counter=0

while 1:
    ser.write('261118191920;1234;37.979618,23.783517;1543252760;5.00;1023;1023;1023;1023;22.00;0;22.00;\n')
    time.sleep(1)
    counter += 1
