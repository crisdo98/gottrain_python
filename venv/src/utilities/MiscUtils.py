import datetime
import time


class MiscUtils:

    @staticmethod
    def get_epoch():
        return int(time.time())

    @staticmethod
    def get_timestamp():
        return str(datetime.datetime.now()).split('.')[0]

    @staticmethod
    def get_datetime():
        return datetime.datetime.today().strftime('%Y%m%d%H%M')
