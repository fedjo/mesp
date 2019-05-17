# Mobile Enviromental Sensing Platform (MESP)

## Scope
1. Apply standards-based data interoperability mechanisms on top of an IoT system
2. Evaluate NGSI and NGSI-LD
3. Extend NGSI-LD in order to model social aspects (e.g. social media data) towards the facilitation of Internet of Everything

## Description
MESP is intented to collect IoT data from a SOURCE, translate them to NGSI, NGSI-LD
and a custom MESP model and send the output to a SINK. Optionally the user can choose to
enhance the NGSI-LD json by classifying images captured from the raspberry camera.
The service logs metrics needed for evaluation between the JSON models to a file.
These metrics are current, voltage, power consumption, translation time,
transmission time, number of request per model and total size of the JSON sent.


The SOURCE could be one of the following:
    - file: collected IoT data, stored to a file
    - serial: IoT data read from directly from a serial port
    - kafka: IoT data read from a Kafka producer

The SINK could be by this time only:
    - orion: translated data are stored on an Orion Context Broker

The CLASSIFICATION process is done using a pretrained Convolutional Neural Network.
The default network has been trained to detect FIRE events. The user is advised
to test the efficiency and accuracy of the classification process before using it.

The METRICS are collected using the `adafruit_ina219` package and are
reported to a CSV file. The default file is `/tmp/metrics.csv` ``

User can specify the specify SOURCE, SINK, CLASSIFICATION, METRICS options on a
configuration file. A default configuration file is placed under `conf/`.

## Prerequisites
The service is intended to run on Raspbian Stretch (ver > 9.0)

It can also run without Kafka support on Raspbian Jessie

## Requirements
- systemd [1]
- python2.7
- pip

## Install
You can use the install script which installs the required python packages
and creates a systemd service.

```
$ ./install.sh
```

## Enable autostart
If you want MESP to automatically start on every boot run the following
as a root user

```
# systemctl enable agent.service
```
