import os
import threading
import tensorflow as tf

from log import logger
from camera import Camera


LOGGER = logger(__name__)


class TensorflowClassifier(threading.Thread):

    def __init__(self, storedir, labels, graph, sourceQueue, sourceLock):
        threading.Thread.__init__(self)

        self.sourceQueue = sourceQueue
        self.sourceLock = sourceLock
        # Just disables the warning, doesn't enable AVX/FMA
        os.environ['TF_CPP_MIN_LOG_LEVEL'] = str(2)

        # Loads label file, strips ocarriaga return
        self.label_lines = [line.rstrip() for line in tf.gfile.GFile(labels)]

        # Camera instance
        self.CAM = Camera(storedir)
        # Unpersists graph from file
        with tf.gfile.FastGFile(graph, 'rb') as f:
            graph_def = tf.GraphDef()
            graph_def.ParseFromString(f.read())
            tf.import_graph_def(graph_def, name='')

        self.sess = tf.Session()

    def run(self):

        while 1:
            img = self.CAM.capture()
            if not (os.path.exists(img)):
                raise ValueError('No such image file: {}.'.format(img))

            # Read in the image_data
            image_data = tf.gfile.FastGFile(img, 'rb').read()

            # Feed image_data as input to the graph and get first prediction
            with self.sess as sess:
                softmaxtensor = sess.graph.get_tensor_by_name('final_result:0')
                predictions = sess.run(softmaxtensor,
                                       {'DecodeJpeg/contents:0': image_data})
                LOGGER.debug(predictions)
                # Sort & show labels of first prediction in order of confidence
                top_k = predictions[0].argsort()[-len(predictions[0]):][::-1]
                info_table = dict()
                for node_id in top_k:
                    human_string = self.label_lines[node_id]
                    score = predictions[0][node_id]
                    info_table[human_string] = score
                    LOGGER.debug('%s (score = %.5f)' % (human_string, score))

            self.sourceLock.acquire()
            self.sourceQueue.put(info_table["field fire"])
            self.sourceLock.release()
