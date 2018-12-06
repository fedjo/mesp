import sys
import time
import threading
import datetime
import requests
import csv

from log import logger
from utils import mesp_dm, ngsi_dm, ngsild_dm

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
        self.raw_post = None
        self.ngsi_post = None
        self.exitFlag = 0

    def _process_data(self, classf_table):
        return self.process_data(self.name, self.q, classf_table)

    def process_data(self, threadName, q, classf_table):
        req_bytes = dict()
        while not self.exitFlag:
            if not self.q.empty():
                self.lock.acquire()
                data = self.q.get()
                self.lock.release()
                LOGGER.debug("{} Got data {}".format(self.name, data))
                LOGGER.debug("Classification: {}".format(classf_table))
                req_bytes = self.raw_post(data, self.schema, classf_table)
                break
            else:
                continue
        return req_bytes

    def run(self):
        LOGGER.info("Starting %s %s" % (self.stype, self.name))
        self.process_data(self.name, self.q, None)
        LOGGER.info("Exiting %s %s" % (self.stype, self.name))


class OrionSink(GeneralSink):

    def __init__(self, threadID, name, q, queuelock, url, schema, metricspath):
        GeneralSink.__init__(self, threadID, 'Orion', name, q, queuelock,
                             schema)
        self.raw_post = self.posttoorion
        self.url = url
        self.metricspath = metricspath
        with open(self.metricspath, 'w+') as csvfile:
                writer = csv.writer(csvfile, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
                writer.writerow(['TIMESTAMP','UNIQUEID', 'TYPE', 'TRANSLATION_TIME',
                                 'TRANSMITION_TIME', 'NO_REQUESTS', 'TOTAL_JSON_SIZE'])


    def getfromorion_id(self, id):
        LOGGER.debug("get from Orion!")
        url = self.url + '/v2/entities/' + id
        headers = {'Accept': 'application/json', 'X-Auth-Token': 'QGIrJsK6sSyKfvZvnsza6DlgjSUa8t'}
        # print url
        response = requests.get(url, headers=headers)
        return response.json()

    def getfromorion_all(self):
        LOGGER.debug("get from Orion!")
        # payload = {'limit': '500'}
        url = self.url + '/v2/entities?limit=500'
        headers = {'Accept': 'application/json', 'X-Auth-Token': 'QGIrJsK6sSyKfvZvnsza6DlgjSUa8t' }
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

    def posttoorion(self, snapshot_raw, schema, classf_table):
        LOGGER.info("Parsing data...")
        LOGGER.info(snapshot_raw)

        batches = schema.split(';')[:-1]
        data = snapshot_raw.split(";")[:-1]

        if len(batches) != len(data):
            LOGGER.debug("Schema and data format are not the same!")
            raise Exception("Schema and data format are not the same!")

        snapshot = {}

        for B, d in zip(batches, data):
            #snapshot[B.lower().replace("#", "_")] = d
            snapshot[B.replace("#", "_")] = d

        LOGGER.info("Post to Orion!")
        LOGGER.info(snapshot)

        # Timestampjust before the tranlation
        before_trans_tmst = datetime.datetime.now()
        # Translate on different models and post data to Orion
        translation = dict()
        # Keep time for each tranlation
        translation_time = dict()

        tmmesp = time.time()
        translation['mesp'] = mesp_dm(snapshot, classf_table, before_trans_tmst)
        translation_time['mesp'] = time.time() - tmmesp

        tmngsi = time.time()
        translation['ngsi'] = ngsi_dm(snapshot, classf_table, before_trans_tmst)
        translation_time['ngsi'] = time.time() - tmngsi

        tmngsild = time.time()
        translation['ngsild'] = ngsild_dm(snapshot, classf_table,before_trans_tmst)
        translation_time['ngsild'] = time.time() - tmngsild


        # LOGGER.info("Entity id: {}".format(str(tmst)))
        transmition_time = dict()
        translation_size = dict()
        if self.url:
            url = self.url + '/v2/entities'
            headers = {'Accept': 'application/json'}
            sess = requests.Session()
            for t, l in translation.iteritems():
                tnsm_time = time.time()
                for json in l:
                    req = requests.Request('POST', url=url, headers=headers, json=json)
                    preq = req.prepare()
                    LOGGER.debug("size of request body sent for %s:" % t)
                    LOGGER.debug(len(preq.body))
                    if t in translation_size:
                        translation_size[t].append( len(preq.headers) + len(preq.body))
                    else:
                        translation_size[t] = [ len(preq.headers) + len(preq.body) ]
                    response = sess.send(preq)
                    transmition_time[t] = time.time() - tnsm_time
                    LOGGER.debug("Response")
                    LOGGER.debug(response.text)

        with open(self.metricspath, 'a+') as cvsfile:
            for t in translation.keys():
                writer = csv.writer(csvfile, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
                row = [str(datetime.datetime.now()), snapshot['UNIQUEID'], t,
                       translation_time[t], transmition_time[t],
                       len(translation_size[t]), sum(translation_size[t])]
                writer.writerow(row)
                csvfile.flush()

        return translation_size
