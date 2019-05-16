# Mobile Enviromental Sensing Platform (MESP)
The scope of this work is to:
1. Applying standards-based data interoperability mechanisms on top of an IoT system
2. Evaluate NGSI and NGSI-LD
3. Extend NGSI-LD in order to model social aspects (e.g. social media data) towards the facilitation of Internet of Everything

## Prerequisites
The service is intended to run on Raspian Stretch (ver > 9.0)

It can also run without Kafka support on Raspbian Jessie

## Requirements
- python2.7
- pip

## Install
You need to have installed systemd [1] on your system

```
$ ./install.sh
```

## Enable autostart
```
# systemctl enable agent.service
```
