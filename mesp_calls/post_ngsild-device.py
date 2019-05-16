import requests
import datetime
from time import time
from time import sleep
import sys


def posttoorion(dm_attr1, dm_attr2, dm_attr3):
    print("parse snapshot")
    print dm_attr1, dm_attr2, dm_attr3

    pts = datetime.datetime.now().strftime('%s')

    url = 'http://localhost:1026/v2/entities?options=keyValues'
    headers = {'Accept': 'application/json'}

    #ngsi format based on:
    # fiware_ngsi_datamodels/specs/Device/Device/schema.json
    # https://fiware.github.io/dataModels/specs/Device/Device/schema.json

    json = {
        {
            "@context": [
                "https://forge.etsi.org/gitlab/NGSI-LD/NGSI-LD/raw/master/coreContext/ngsi-ld-core-context.json",
                "https://raw.githubusercontent.com/GSMADeveloper/NGSI-LD-Entities/master/examples/Device-context.jsonld"
            ],
            "id": "urn:ngsi-ld:Device:RaspberryPi:d9e7-43cd-9c68-1111",
            "type": "Device",
            "source": "GSMA",
            "dataProvider": "GSMA",
            "entityVersion": 2.0,
            "deviceModel": {
                "type": "Relationship",
                "object": "urn:ngsi-ld:DeviceModel:RaspberryPi:d9e7-43cd-9c68-1111"
            },
            "serialNumber": {
                "type": "Property",
                "value": "X123456789C"
            },
            "supplierName": {
                "type": "Property",
                "value": "RaspberryPi, Inc."
            },
            "countryOfManufacture": {
                "type": "Property",
                "value": "India"
            },
            "factory": {
                "type": "Property",
                "value": "56A8"
            },
            "firstUsedAt": {
                "type": "Property",
                "value": "2018-05-04T11:18:16Z"
            },
            "lastCalibrationAt": {
                "type": "Property",
                "value": "2018-05-04T10:18:16Z"
            },
            "installedAt": {
                "type": "Property",
                "value": "2018-05-04T10:18:16Z"
            },
            "manufacturedAt": {
                "type": "Property",
                "value": "2017-05-04T10:18:16Z"
            },
            "description": {
                "type": "Property",
                "value": "MESP Gateway"
            },
            "owner": {
                "type": "Relationship",
                "object": [
                    "urn:ngsi-ld:Person:6babd63c-46e6-11e8-aaf6-abd6d570d4ec",
                    "urn:ngsi-ld:Organization:76f72898-46e6-11e8-8340-53a92642d82a"
                ]
            },
            "hardwareVersion": {
                "type": "Property",
                "value": "1.2"
            },
            "firmwareVersion": {
                "type": "Property",
                "value": "2.8.56"
            },
            "softwareVersion": {
                "type": "Property",
                "value": "2.5.11"
            },
            "osVersion": {
                "type": "Property",
                "value": "8.1"
            },
            "supportedProtocols": {
                "type": "Property",
                "value": [
                    "HTTP",
                    "HTTPS",
                    "FTP"
                ],
                "observedAt": "2017-05-04T12:30:00Z"
            },
            "location": {
                "type": "GeoProperty",
                "value": {
                    "type": "Point",
                    "coordinates": [
                        37.9877,
                        23.7319
                    ]
                }
            },
            "online": {
                "type": "Property",
                "value": true,
                "observedAt": "2017-05-04T12:30:00Z"
            },
            "status": {
                "type": "Property",
                "value": "SC1001",
                "observedAt": "2017-05-04T12:30:00Z"
            },
            "batteryLevel": {
                "type": "Property",
                "value": 0.7,
                "observedAt": "2017-05-04T12:30:00Z"
            }
        }

    }


    print json

    json_bytes = sys.getsizeof(json)
    headers_bytes = sys.getsizeof(headers)
    total = json_bytes + headers_bytes
    print json_bytes,headers_bytes, total

    response = requests.post(url, headers=headers, json=json)
    print(str(response))
    return response

dm_attr1 = "RaspberryPi3B"
dm_attr2 = "xx"
dm_attr3 = "yy"

posttoorion(dm_attr1, dm_attr2, dm_attr3)

