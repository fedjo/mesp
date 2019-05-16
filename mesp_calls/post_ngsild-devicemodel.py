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

    #ngsi-ld format
    json = {
            "@context": [
                "https://forge.etsi.org/gitlab/NGSI-LD/NGSI-LD/raw/master/coreContext/ngsi-ld-core-context.json",
                "https://raw.githubusercontent.com/GSMADeveloper/NGSI-LD-Entities/master/examples/Device-Model-context.jsonld"
            ],
            "id": "urn:ngsi-ld:DeviceModel:RaspberryPi:d9e7-43cd-9c68-2222",
            "type": "DeviceModel",
            "source": "GSMA",
            "dataProvider": "GSMA",
            "entityVersion": 2.0,
            "name": {
                "type": "Property",
                "value": "RaspberryPi3B+"
            },
            "doc": {
                "type": "Property",
                "value": {
                    "@value": "https://www.raspberrypi.org/products/raspberry-pi-3-model-b-plus/",
                    "@type": "https://schema.org/url"
                }
            },
            "category": {
                "type": "Property",
                "value": [
                    "computer"
                ]
            },
            "description": {
                "type": "Property",
                "value": "The gateway component of the Mobile Environmental Sensing Platform-MESP"
            },
            "manufacturerName": {
                "type": "Property",
                "value": "Raspberry_Pi_Foundation"
            },
            "brandName": {
                "type": "Property",
                "value": "RaspberryPi"
            },
            "root": {
                "type": "Property",
                "value": "True"
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

dm_attr1 = ""
dm_attr2 = ""
dm_attr3 = ""

posttoorion(dm_attr1, dm_attr2, dm_attr3)

