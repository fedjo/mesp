import sys
import time
import threading
import datetime
import requests

from utils import translate


class GeneralSink(threading.Thread):
    def __init__(self, threadID, stype, name, q, queuelock, schema, logger):
        threading.Thread.__init__(self)
        self.threadID = threadID
        self.stype = stype
        self.name = name
        self.q = q
        self.lock = queuelock
        self.schema = schema
        self.logger = logger
        self.post = None
        self.exitFlag = 0

    def process_data(self, threadName, q):
        while not self.exitFlag:
            if not self.q.empty():
                self.lock.acquire()
                data = self.q.get()
                self.lock.release()
                # self.logger.debug("%s Got data %s" % (threadName, data))
                self.post(data, self.schema)
            else:
                continue

    def run(self):
        self.logger.info("Starting %s %s" % (self.stype, self.name))
        self.process_data(self.name, self.q)
        self.logger.info("Exiting %s %s" % (self.stype, self.name))


class OrionSink(GeneralSink):

    def __init__(self, threadID, name, q, queuelock, url, schema, logger):
        self.logger = logger
        GeneralSink.__init__(self, threadID, 'Orion', name, q, queuelock,
                             schema, logger)
        self.post = self.posttoorion
        self.cross_ref_unique_ids = []
        self.url = url

    def getfromorion_id(self, id):
        self.logger.debug("get from Orion!")
        url = self.url + '/v2/entities/' + id
        headers = {'Accept': 'application/json', 'X-Auth-Token': 'QGIrJsK6sSyKfvZvnsza6DlgjSUa8t'}
        # print url
        response = requests.get(url, headers=headers)
        return response.json()

    def getfromorion_all(self):
        self.logger.debug("get from Orion!")
        # payload = {'limit': '500'}
        url = self.url + '/v2/entities?limit=500'
        headers = {'Accept': 'application/json', 'X-Auth-Token': 'QGIrJsK6sSyKfvZvnsza6DlgjSUa8t' }
        # print url
        response = requests.get(url, headers=headers)
        # print response.json()
        return response.json()

    def post_all(self, measurments_list):
        self.logger.debug("reading measurment")
        for measurment_one in measurments_list:
            time.sleep(1)
            self.logger.debug(measurment_one)
            self.logger.debug("posting")
            self.posttoorion(measurment_one)
            self.logger.debug("done")

        self.logger.debug("finished")

    def retrieve_id(self):
        for id in self.cross_ref_unique_ids:
            self.logger.debug("retrieving : " + str(id))
            res = self.getfromorion_id(id)
            self.logger.debug(res['id'])
            if res['id'] == id:
                self.logger.debug("found")
            else:
                self.logger.debug("not found")

    def posttoorion(self, snapshot_raw, schema):
        self.logger.info("Parsing data...")
        self.logger.info(snapshot_raw)

        batches = schema.split(';')[:-1]
        data = snapshot_raw.split(";")[:-1]

        if len(batches) != len(data):
            self.logger.debug("Schema and data format are not the same!")
            raise Exception("Schema and data format are not the same!")

        experimental_results = {}
        ts1_received = time.time()
        experimental_results["ts1_received"] = ts1_received

        snapshot = {}

        for B, d in zip(batches, data):
            snapshot[B.lower()] = d

        self.logger.info("Post to Orion!")
        self.logger.info(snapshot)
        # print("TIME")
        pts = datetime.datetime.now().strftime('%s')
        # print(pts)

        json = translate(snapshot, pts, self.logger)
        ts2 = time.time()
        self.logger.debug(json)
        # print "posting"

        translation_time = ts2-ts1_received
        volume = sys.getsizeof(json)
        self.logger.debug("translation time")
        self.logger.info("entity id, volume, translation_time")
        self.logger.info("{}, {}, {}".format(str(pts), volume, translation_time))

        if self.url:
            url = self.url + '/v2/entities'
            headers = {'Accept': 'application/json', 'X-Auth-Token': 'QGIrJsK6sSyKfvZvnsza6DlgjSUa8t'}
            # print url
            response = requests.post(url, json=json)

            self.logger.debug("response")
            self.logger.debug(response.text)

        self.cross_ref_unique_ids.append(str(pts))
        self.logger.debug("list of ids translated and send:")
        self.logger.debug(str(pts))
