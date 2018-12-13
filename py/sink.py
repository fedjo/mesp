import time
import threading
import datetime
import requests
import csv

from log import logger
from utils import mesp_dm, mesp_dm2, ngsi_dm, ngsild_dm
from metrics import ConsumptionSource

LOGGER = logger(__name__)


class GeneralSink(threading.Thread):
    def __init__(self, threadID, stype, name, q, queuelock, schema):
        threading.Thread.__init__(self)
        self.threadID = threadID
        self.stype = stype
        self.name = name
        self.q = q
        self.lock = queuelock
        self.schema = schema
        self.sink = None
        self.exitFlag = 0

    def process_data(self, threadName, q):
        req_bytes = dict()
        while not self.exitFlag:
            if not self.q.empty():
                self.lock.acquire()
                rawdata = self.q.get()
                self.lock.release()
                LOGGER.debug("{} Got data {}".format(self.name, rawdata))

                # process rawdata
                LOGGER.info("Parsing data...")

                # Get rawdata
                batches = self.schema.split(';')[:-1]
                data = rawdata['raw'].decode('utf-8').split(";")[:-1]

                # Check if they fit with the schema
                if len(batches) != len(data):
                    LOGGER.debug("Schema and data format are not the same!")
                    continue

                # Re-pack all data to snapshot dict
                snapshot = {}
                for B, d in zip(batches, data):
                    # snapshot[B.lower().replace("#", "_")] = d
                    snapshot[B.replace("#", "_")] = d
                if "score" in rawdata:
                    snapshot["score"] = rawdata["score"]
                else:
                    snapshot["score"] = 0
                if "imgname" in rawdata:
                    snapshot["imgname"] = rawdata["imgname"]
                else:
                    snapshot["imgname"] = ''

                req_bytes = self.sink(snapshot)
        return req_bytes

    def run(self):
        LOGGER.info("Starting %s %s" % (self.stype, self.name))
        self.process_data(self.name, self.q)
        LOGGER.info("Exiting %s %s" % (self.stype, self.name))


class OrionSink(GeneralSink):

    def __init__(self, threadID, name, q, queuelock, url, schema, metricspath):
        GeneralSink.__init__(self, threadID, 'Orion', name, q, queuelock,
                             schema)
        self.sink = self.posttoorion
        self.url = url
        self.metricspath = metricspath
        with open(self.metricspath, 'w+') as csvfile:
                writer = csv.writer(csvfile, delimiter=',', quotechar='"',
                                    quoting=csv.QUOTE_MINIMAL)
                writer.writerow(['TIMESTAMP', 'UNIQUEID', 'TYPE',
                                 'TRANSLATION_TIME', 'TRANSMITION_TIME',
                                 'NO_REQUESTS', 'TOTAL_JSON_SIZE',
                                 'VOLTAGE (V)', 'Current(mA)', 'Power(mW)'])

    def getfromorion_id(self, id):
        LOGGER.debug("get from Orion!")
        url = self.url + '/v2/entities/' + id
        headers = {'Accept': 'application/json',
                   'X-Auth-Token': 'QGIrJsK6sSyKfvZvnsza6DlgjSUa8t'}
        # print url
        response = requests.get(url, headers=headers)
        return response.json()

    def getfromorion_all(self):
        LOGGER.debug("get from Orion!")
        # payload = {'limit': '500'}
        url = self.url + '/v2/entities?limit=500'
        headers = {'Accept': 'application/json',
                   'X-Auth-Token': 'QGIrJsK6sSyKfvZvnsza6DlgjSUa8t'}
        # print url
        response = requests.get(url, headers=headers)
        # print response.json()
        return response.json()

    def post_all(self, measurments_list):
        LOGGER.debug("reading measurment")
        for measurment_one in measurments_list:
            time.sleep(1)
            LOGGER.debug(measurment_one)
            LOGGER.debug("posting")
            self.posttoorion(measurment_one)
            LOGGER.debug("done")

        LOGGER.debug("finished")

    def posttoorion(self, snapshot):

        LOGGER.debug('Post to Orion')
        LOGGER.info(snapshot)
        consumption = ConsumptionSource()
        # Timestampjust before the tranlation
        before_trans_tmst = datetime.datetime.now()
        # Translate on different models and post data to Orion
        translation = dict()
        # Keep time for each tranlation
        translation_time = dict()

        consumption.start()
        tmmesp = time.time()
        translation['mesp'] = mesp_dm2(snapshot, before_trans_tmst)
        translation_time['mesp'] = time.time() - tmmesp
        LOGGER.debug("Here")    
        tmngsi = time.time()
        translation['ngsi'] = ngsi_dm(snapshot, before_trans_tmst)
        translation_time['ngsi'] = time.time() - tmngsi

        tmngsild = time.time()
        translation['ngsild'] = ngsild_dm(snapshot, before_trans_tmst)
        translation_time['ngsild'] = time.time() - tmngsild

        # LOGGER.info("Entity id: {}".format(str(tmst)))
        transmition_time = dict()
        translation_size = dict()
        if self.url:
            url = self.url + '/v2/entities'
            headers = {'Accept': 'application/json'}
            sess = requests.Session()
            for t, l in translation.items():
                # Hack for ngsi-lg json objects
                if t == 'ngsild':
                    url += '?options=keyValues'

                tnsm_time = time.time()
                for json in l:
                    req = requests.Request('POST', url=url, headers=headers,
                                           json=json)
                    preq = req.prepare()
                    LOGGER.debug("size of request body sent for %s:" % t)
                    LOGGER.debug(len(preq.body))
                    if t in translation_size:
                        translation_size[t].append(len(preq.headers) +
                                                   len(preq.body))
                    else:
                        translation_size[t] = [len(preq.headers) +
                                               len(preq.body)]
                    response = sess.send(preq)
                    transmition_time[t] = time.time() - tnsm_time
                    LOGGER.debug("Response")
                    LOGGER.debug(response.text)
        
        consumption.stop()
        (v, c, p) = consumption.get()
        with open(self.metricspath, 'a+') as csvfile:
            for t in translation.keys():
                writer = csv.writer(csvfile, delimiter=',', quotechar='"',
                                    quoting=csv.QUOTE_MINIMAL)
                row = [str(datetime.datetime.now()), snapshot['UNIQUEID'], t,
                       translation_time[t], transmition_time[t],
                       len(translation_size[t]), sum(translation_size[t]),
                       v, c, p]
                writer.writerow(row)
                csvfile.flush()

        return translation_size
