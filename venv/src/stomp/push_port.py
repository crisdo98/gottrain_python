import gzip
import io

import stompest
import pyactivemq


class MyListener(object):
    #
    # def __init__ (self, conn):
    #       self._conn = conn

    def on_error(self, headers, message):
        print('received an error %s' % message)

    def on_message(self, headers, message):
        fp = gzip.GzipFile(fileobj=io.StringIO(message))
        text = fp.readlines()
        fp.close()
        print('%s\n' % text)

    #       self._conn.ack(id=headers['message-id'], subscription=headers['subscription'])


conn = stomp.Connection([('darwin-dist-44ae45.nationalrail.co.uk', 61613)])

conn.set_listener('', MyListener())
conn.start()
conn.connect(username='DARWIN37b97bab-87cd-440e-bbee-2c82ce33136b', passcode='32be0016-15e5-4d9f-a492-b250ed5fadd6', wait=False)

conn.subscribe(destination='/queue/darwin.pushport-v16', id=1, ack='auto')

# conn.send(body=' '.join(sys.argv[1:]), destination='')

# mydata = raw_input('Prompt :')

conn.disconnect()
