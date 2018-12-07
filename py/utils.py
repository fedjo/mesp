import requests
import datetime

from log import logger

LOGGER = logger(__name__)


def calltoopenweathermap():
    LOGGER.debug("Goodbye, World!")
    weather_url = 'http://api.openweathermap.org/data/2.5/forecast?lat=38.303860&lon=23.730180&cnt=5&appid=3a87a263c645ea5eb18ad7417be4cb0d'
    LOGGER.debug(weather_url)
    response = requests.post(weather_url)
    LOGGER.debug(response.json())


def mesp_dm(snapshot_dict, timestamp):

    json = {

        "id": snapshot_dict["UNIQUEID"],
        "type": "MespMeasurement",
        "translation_timestamp": {
            "value": timestamp.strftime('%s'),
            "type": "time"
        }
    }

    value = dict()
    types = {
        'nodeid': 'id',
        'gps_location': 'GPS',
        'humidity': 'Number',
        'flame': 'Number',
        'temp-air': 'Number',
        'gas': 'Number',
        'temp-soil': 'Number',
        'uniqueid': 'time',
        'epoch': 'time',
        'score': 'Number'
    }
    for k, v in snapshot_dict.iteritems():
        if '_' in k:
            k = k[:-2]
        k = k.lower()
        if 'gps' in k:
            k = 'gps_location'
        # if 'epoch' in k:
        #    v = datetime.datetime.utcfromtimestamp(int(v))
        value[k] = {
            "value": v,
            "type": types[k]
        }
        json.update(value)

    return [json]


def ngsi_dm(snapshot_dict, timestamp):

    # UNIQUEID = timestamp.strftime('%s')
    UNIQUEID = snapshot_dict["UNIQUEID"]
    # EPOCH = snapshot_dict("EPOCH")
    EPOCH = str(datetime.datetime.utcfromtimestamp(
        int(snapshot_dict["EPOCH"])))
    GAS_1 = snapshot_dict["GAS_1"]
    # FLAME_0 = snapshot_dict["FLAME_0"]
    FLAME_1 = snapshot_dict["FLAME_1"]
    # FLAME_2 = snapshot_dict["FLAME_2"]
    # FLAME_3 = snapshot_dict["FLAME_3"]
    TEMP_SOIL_1 = snapshot_dict["TEMP-SOIL_1"]
    HUMIDITY_1 = snapshot_dict["HUMIDITY_1"]
    TEMP_AIR_1 = snapshot_dict["TEMP-AIR_1"]
    GPS_X = float(snapshot_dict["GPS_1"].split(',')[0])
    GPS_Y = float(snapshot_dict["GPS_1"].split(',')[1])
    SCORE = str(snapshot_dict["score"])

    json_airquality = {
        "id": "AirQualityObserved:ntua:" + UNIQUEID,
        "type": "AirQualityObserved",
        "dateObserved": {
            "value": EPOCH,
            "type": "Date"
        },
        "airQualityLevel": {
            "value": "moderate",
            "type": "text"
        },
        "CO": {
            "value": GAS_1,
            "metadata": {
                "unitCode": {
                    "value": "GP"
                }
            }
        },
        "NO": {
            "value": GAS_1,
            "type": "text",
            "metadata": {
                "unitCode": {
                    "value": "GQ"
                }
            }
        },
        "location": {
            "type": "geo:json",
            "value": {
                "type": "Point",
                "coordinates": [GPS_X, GPS_Y]
            }
        },
        "airQualityIndex": {
            "value": 65,
            "type": "text"
        }
    }

    json_forest_fire = {
        "id": "Alert:security:forestFire:" + UNIQUEID,
        "type": "Alert",
        "category": {
            "value": "security",
            "type": "text"
        },
        "subCategory": {
            "value": "forestFire",
            "type": "text"
        },
        "severity": {
            "value": FLAME_1,
            "type": "text"
        },
        "location": {
            "value": {
                "type": "Point",
                "coordinates": [GPS_X, GPS_Y]
            },
            "type": "geo:json"
        },
        "dateIssued": {
            "value": str(timestamp),
            "type": "date"
        },
        "description": {
            "value": "forest fire detected in the area of xxxx",
            "type": "text"
        },
        "alertSource": {
            "value": "Based on iMESP image classification engine",
            "type": "text"
        }
    }

    json_forest_fire_image = {
        "id": "Alert:security:forestFireImage:" + UNIQUEID,
        "type": "Alert",
        "category": {
            "value": "security",
            "type": "text"
        },
        "subCategory": {
            "value": "forestFire",
            "type": "text"
        },
        "severity": {
            "value": SCORE,
            "type": "text"
        },
        "location": {
            "value": {
                "type": "Point",
                "coordinates": [GPS_X, GPS_Y]
            },
            "type": "geo:json"
        },
        "dateIssued": {
            "value": str(timestamp),
            "type": "date"
        },
        "description": {
            "value": "forest fire detected in the area of xxxx",
            "type": "text"
        },
        "alertSource": {
            "value": "Based on iMESP image classification engine",
            "type": "text"
        }
    }

    json_greenspace = {
        "id": "Greenspace:rafina:1" + UNIQUEID,
        "type": "GreenspaceRecord",

        "location": {
            "value": {
                "type": "Point",
                "coordinates": [
                    GPS_X,
                    GPS_Y
                ]
            },
            "type": "geo:json"
        },

        "dateObserved": {
            "value": EPOCH,
            "type": "date"
        },
        "soilTemperature": {
            "value": TEMP_SOIL_1,
            "type": "Number"
        },
        "relativeHumidity": {
            "value": HUMIDITY_1,
            "type": "Number"
        },

        "Temperature": {
            "value": TEMP_AIR_1,
            "type": "Number"
        },
        "refAirQualityObserved": {
            "value": "AirQualityObserved:ntua:" + UNIQUEID,
            "type": "Reference"
        },
        "refAlert": {
            "value": ["Alert:security:forestFire:" + UNIQUEID,
                      "Alert:weather:fireRisk:123"],
            "type": "Reference"
        },
        "refDevice": {
            "value": ["urn:ngsi:Device:RaspberryPi:d9e7-43cd-9c68-1111",
                      "urn:ngsi:Device:Arduino:d9e7-43cd-9c68-1111"],
            "type": "Reference"
        }
    }

    return [json_airquality, json_forest_fire,
            json_forest_fire_image, json_greenspace]


def ngsild_dm(snapshot_dict, timestamp):

    UNIQUEID = timestamp.strftime('%s')
    UNIQUEID = snapshot_dict["UNIQUEID"]
    EPOCH = str(datetime.datetime.utcfromtimestamp(
        int(snapshot_dict["EPOCH"])))
    GAS_1 = snapshot_dict["GAS_1"]
    # FLAME_0 = snapshot_dict["FLAME_0"]
    FLAME_1 = snapshot_dict["FLAME_1"]
    # FLAME_2 = snapshot_dict["FLAME_2"]
    # FLAME_3 = snapshot_dict["FLAME_3"]
    TEMP_SOIL_1 = snapshot_dict["TEMP-SOIL_1"]
    HUMIDITY_1 = snapshot_dict["HUMIDITY_1"]
    TEMP_AIR_1 = snapshot_dict["TEMP-AIR_1"]
    GPS_X = float(snapshot_dict["GPS_1"].split(',')[0])
    GPS_Y = float(snapshot_dict["GPS_1"].split(',')[1])
    SCORE = str(snapshot_dict["score"])

    json_airquality = {
        "@context": [
            "https://forge.etsi.org/gitlab/NGSI-LD/NGSI-LD/raw/master/coreContext/ngsi-ld-core-context.json",
            "https://raw.githubusercontent.com/GSMADeveloper/NGSI-LD-Entities/master/examples/Air-Quality-Observed-context.jsonld"
        ],
        "id": "urn:ngsi-ld:AirQualityObserved:" + UNIQUEID,
        "type": "AirQualityObserved",
        "entityVersion": 2.0,
        "name": {
            "type": "Property",
            "value": "MESP"
        },
        "location": {
            "type": "GeoProperty",
            "value": {
                "type": "Point",
                "coordinates": [GPS_X, GPS_Y]
            }
        },
        "observedAt": {
            "type": "Property",
            "value": EPOCH
        },
        "airQualityIndex": {
            "type": "Property",
            "value": {
                "value": 65,
                "unitText": "US EPA AQI"
            },
            "observedAt": EPOCH
        },
        "CO": {
            "type": "Property",
            "value": {
                "value": GAS_1,
                "unitText": "microgramme per cubic metre"
            },
            "unitCode": "GQ",
            "observedAt": EPOCH
        },
        "NO": {
            "type": "Property",
            "value": {
                "value": GAS_1,
                "unitText": "microgramme per cubic metre"
            },
            "unitCode": "GQ",
            "observedAt": EPOCH
        }
    }

    json_forest_fire = {
        "@context": [
            "https://forge.etsi.org/gitlab/NGSI-LD/NGSI-LD/raw/master/coreContext/ngsi-ld-core-context.json",
            "https://raw.githubusercontent.com/nikoskal/mesp/mdpi/etsi_ngsild_datamodels/specs/Alert/schema.json"
        ],
        "id": "urn:ngsi-ld:Alert:security:" + UNIQUEID,
        "type": "Alert",
        "source": "GSMA",
        "dataProvider": "GSMA",
        "entityVersion": 2.0,

        "category": {
            "type": "Property",
            "value": "security"
        },
        "subCategory": {
            "value": "forestFire",
            "type": "text"
        },
        "severity": {
            "value": FLAME_1,
            "type": "text"
        },
        "location": {
            "value": {
                "type": "Point",
                "coordinates": [GPS_X, GPS_Y]
            },
            "type": "geo:json"
        },
        "dateIssued": {
            "value": str(timestamp),
            "type": "date"
        },
        "description": {
            "value": "forest fire detected in the area of xxxx",
            "type": "text"
        },
        "alertSource": {
            "value": "Based on iMESP image classification engine",
            "type": "text"
        }
    }

    json_forest_fire_image = {
        "@context": [
            "https://forge.etsi.org/gitlab/NGSI-LD/NGSI-LD/raw/master/coreContext/ngsi-ld-core-context.json",
            "https://raw.githubusercontent.com/nikoskal/mesp/mdpi/etsi_ngsild_datamodels/specs/Alert/schema.json"
        ],
        "id": "urn:ngsi-ld:Alert:security:image:" + UNIQUEID,
        "type": "Alert",
        "source": "GSMA",
        "dataProvider": "GSMA",
        "entityVersion": 2.0,

        "category": {
            "type": "Property",
            "value": "security"
        },
        "subCategory": {
            "value": "forestFire",
            "type": "text"
        },
        "severity": {
            "value": SCORE,
            "type": "text"
        },
        "location": {
            "value": {
                "type": "Point",
                "coordinates": [GPS_X, GPS_Y]
            },
            "type": "geo:json"
        },
        "dateIssued": {
            "value": str(timestamp),
            "type": "date"
        },
        "description": {
            "value": "forest fire detected in the area of xxxx",
            "type": "text"
        },
        "alertSource": {
            "value": "Based on iMESP image classification engine",
            "type": "text"
        }
    }

    json_greenspace = {
        "@context": [
            "https://forge.etsi.org/gitlab/NGSI-LD/NGSI-LD/raw/master/coreContext/ngsi-ld-core-context.json",
            "https://raw.githubusercontent.com/GSMADeveloper/NGSI-LD-Entities/master/examples/Agri-Parcel-Record-context.jsonld"
        ],

        "id": "urn:ngsi-ld:AgriParcelRecord:" + UNIQUEID,
        "type": "Agri-Parcel",

        "location": {
            "value": {
                "type": "Point",
                "coordinates": [
                    GPS_X,
                    GPS_Y
                ]
            },
            "type": "geo:json"
        },

        "dateObserved": {
            "value": EPOCH,
            "type": "date"
        },

        "soilTemperature": {
            "type": "Property",
            "value": TEMP_SOIL_1,
            "unitCode": "CEL",
            "observedAt": EPOCH
        },
        "relativeHumidity": {
            "type": "Property",
            "value": HUMIDITY_1,
            "unitCode": "CEL",
            "observedAt": EPOCH
        },

        "Temperature": {
            "type": "Property",
            "value": TEMP_AIR_1,
            "unitCode": "CEL",
            "observedAt": EPOCH
        },
        "refAirQualityObserved": {
            "value": "urn:ngsi-ld:AirQualityObserved:" + UNIQUEID,
            "type": "Reference"
        },
        "refAlert": {
            "value": ["urn:ngsi-ld:Alert:security:" + UNIQUEID,
                      "Alert:weather:fireRisk:123"],
            "type": "Reference"
        },
        "refDevice": {
            "value": ["urn:ngsi:Device:RaspberryPi:d9e7-43cd-9c68-1111",
                      "urn:ngsi:Device:Arduino:d9e7-43cd-9c68-1111"],
            "type": "Reference"
        }
    }
    return [json_airquality, json_forest_fire,
            json_forest_fire_image, json_greenspace]
