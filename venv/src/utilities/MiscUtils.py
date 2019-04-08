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
    def get_datetime_now():
        return datetime.datetime.today().strftime('%Y%m%d%H%M')

    @staticmethod
    def get_datetime(unix_time):
        return datetime.datetime.utcfromtimestamp(int(unix_time) / 1000)
