#from picamera import PiCamera
from time import sleep


class Camera():

    def __init__(self, imgdir):
        #self.camera = PiCamera()
        self.idx = 0
        self.imgdir = imgdir

    def capture(self):

        imgpath = "{}/{}.jpg".format(self.imgdir, str(self.idx))
        #self.camera.start_preview()
        sleep(5)
        #self.camera.capture(imgpath)
        #self.camera.stop_preview()
        return imgpath
