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
        "id": "device-9845A",
        "type": "Device",
        "category": ["computer"],
        "controlledProperty": ["fillingLevel","temperature"],
        "controlledAsset": ["wastecontainer-Osuna-100"],
        "ipAddress": ["192.14.56.78"],
        "mcc": "214",
        "mnc": "07",
        "batteryLevel": 0.75,
        "serialNumber": "9845A",
        "refDeviceModel": "myDevice-wastecontainer-sensor-345",
        "value": "l%3D0.22%3Bt%3D21.2",
        "deviceState": "ok",
        "dateFirstUsed": "2014-09-11T11:00:00Z",
        "owner": ["http://person.org/leon"]
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

