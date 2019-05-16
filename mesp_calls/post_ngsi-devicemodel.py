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
    # fiware_ngsi_datamodels/specs/Device/DeviceModel/schema.json
    # https://fiware.github.io/dataModels/specs/Device/DeviceModel/schema.json

    json = {
        "id": "myDevice-wastecontainer-sensor-345",
        "type": "DeviceModel",
        "name": "myDevice Sensor for Containers 345",
        "brandName": "myDevice",
        "modelName": dm_attr1,
        "manufacturerName": "myDevice Inc.",
        "category": ["computer"],
        "function": ["sensing"],
        "controlledProperty": ["fillingLevel", "temperature"]
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

