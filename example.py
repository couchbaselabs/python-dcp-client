
import json
import threading
import time

from dcp import DcpClient, ResponseHandler


class MyHandler(ResponseHandler):

    def __init__(self):
        ResponseHandler.__init__(self)
        self.lock = threading.Lock()
        self.count = 0

    def mutation(self, response):
        self.lock.acquire()
        #print "Mutation: ", response
        self.count +=1
        self.lock.release()

    def deletion(self, response):
        self.lock.acquire()
        #print "Deletion: ", response
        self.count += 1
        self.lock.release()

    def marker(self, response):
        self.lock.acquire()
        #print "Marker: ", response
        self.lock.release()

    def stream_end(self, response):
        self.lock.acquire()
        #print "Stream End: ", response
        self.lock.release()

    def get_num_items(self):
        return self.count

def main():
    handler = MyHandler()
    client = DcpClient()
    client.connect('172.23.105.195', 8091, 'bucket-1', 'Administrator', 'password',
                   handler)
    for i in range(8):
        result = client.add_stream(i, 0, 0, 10, 0, 0, 0)
        if result['status'] != 0:
            print 'Stream request to vb %d failed dur to error %d' %\
                (i, result['status'])

    while handler.has_active_streams():
        time.sleep(.25)

    print handler.get_num_items()
    client.close()
    #print json.dumps(client.nodes, sort_keys=True, indent=2)
    #print json.dumps(client.buckets, sort_keys=True, indent=2)

if __name__ == "__main__":
    main()

