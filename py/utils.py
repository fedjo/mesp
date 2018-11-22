import requests


def calltoopenweathermap(logger):
    logger.debug("Goodbye, World!")
    weather_url = 'http://api.openweathermap.org/data/2.5/forecast?lat=38.303860&lon=23.730180&cnt=5&appid=3a87a263c645ea5eb18ad7417be4cb0d'
    logger.debug(weather_url)
    response = requests.post(weather_url)
    logger.debug(response.json())


def translate(snapshot_dict, timestamp, logger):

    json = {

        "id": str(timestamp),
        "type": "Sensor123",
        "translation_timestamp": {
            "value": str(timestamp),
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
            'epoch': 'time'
    }
    for k,v in snapshot_dict.iteritems():
        if '#' in k:
            k = k[:-2]
        if 'gps' in k:
            k = 'gps_location'
        value[k] = {
            "value": v,
            "type": types[k]
        }
        json.update(value)
    logger.debug(json)
    return json
