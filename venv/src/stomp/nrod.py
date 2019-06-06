import json

from stompest.config import StompConfig
from stompest.protocol import StompSpec
from stompest.sync import Stomp

from utilities.MiscUtils import MiscUtils

CONFIG = StompConfig('tcp://datafeeds.networkrail.co.uk:61618')
QUEUE1 = '/topic/TD_ALL_SIG_AREA'
# QUEUE2 = '/topic/TD_LNW_WMC_SIG_AREA'

if __name__ == '__main__':
    client = Stomp(CONFIG)

    client._config.login = "chris.solimo@gmail.com"
    client._config.passcode = "ChangeMe123@"
    client.connect()
    client.subscribe(QUEUE1, {StompSpec.ACK_HEADER: StompSpec.ACK_CLIENT_INDIVIDUAL})
    # client.subscribe(QUEUE2, {StompSpec.ACK_HEADER: StompSpec.ACK_CLIENT_INDIVIDUAL})
    while True:
        frame = client.receiveFrame()
        # print(frame.body)
        b = json.loads(frame.body)

        for c in b:
            # print("Key: ", c, "\n")
            for k1, v1 in c.items():
                # print(k1,v1)

                if k1 == "CA_MSG":
                    area_id = v1["area_id"]
                    _from = v1["from"]
                    msg_type = v1["msg_type"]
                    descr = v1["descr"]
                    _to = v1["to"]
                    time = MiscUtils.get_datetime(v1["time"])
                    print(k1, time, area_id, descr, _to, _from, msg_type, "\n")

                elif k1 == "CB_MSG":
                    for key, value in c.items():
                        area_id = v1["area_id"]
                        _from = v1["from"]
                        msg_type = v1["msg_type"]
                        descr = v1["descr"]
                        time = MiscUtils.get_datetime(v1["time"])
                        print(k1, time, area_id, descr, _from, msg_type, "\n")

                elif k1 == "CC_MSG":
                    area_id = v1["area_id"]
                    _to = v1["to"]
                    msg_type = v1["msg_type"]
                    descr = v1["descr"]
                    time = MiscUtils.get_datetime(v1["time"])
                    print(k1, time, area_id, descr, _to, msg_type, "\n")

                elif k1 == "CT_MSG":
                    area_id = v1["area_id"]
                    report_time = v1["report_time"]
                    msg_type = v1["msg_type"]
                    time = MiscUtils.get_datetime(v1["time"])
                    print(k1, time, area_id, report_time, msg_type, "\n")

                elif k1 == "SF_MSG":
                    area_id = v1["area_id"]
                    msg_type = v1["msg_type"]
                    address = v1["address"]
                    data = v1["data"]
                    time = MiscUtils.get_datetime(v1["time"])
                    print(k1, time, area_id, data, address, msg_type, "\n")

                elif k1 == "SG_MSG":
                    area_id = v1["area_id"]
                    msg_type = v1["msg_type"]
                    address = v1["address"]
                    data = v1["data"]
                    time = MiscUtils.get_datetime(v1["time"])
                    print(k1, time, area_id, data, address, msg_type, "\n")

                elif k1 == "SH_MSG":
                    area_id = v1["area_id"]
                    msg_type = v1["msg_type"]
                    address = v1["address"]
                    data = v1["data"]
                    time = MiscUtils.get_datetime(v1["time"])
                    print(k1, time, area_id, data, address, msg_type, "\n")
                else:
                    print(k1, "Unknown Message")
                    exit()

        # exit()
        client.ack(frame)
    # client.disconnect()
