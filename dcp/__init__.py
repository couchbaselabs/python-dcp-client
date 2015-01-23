
import operation
import threading
import time

from cluster import RestClient
from connection import ConnectionManager
from constants import FLAG_OPEN_PRODUCER
from dcp_exception import ConnectedException
from operation import (CountdownLatch, Control, OpenConnection, SaslPlain,
                       StreamRequest)

class ResponseHandler():

    def __init__(self):
        self.active_streams = 0

    def mutation(self, response):
        raise NotImplementedError("Subclass must implement abstract method")

    def deletion(self, response):
        raise NotImplementedError("Subclass must implement abstract method")

    def marker(self, response):
        raise NotImplementedError("Subclass must implement abstract method")

    def stream_end(self, response):
        raise NotImplementedError("Subclass must implement abstract method")

    def has_active_streams(self):
        assert self.active_streams >= 0
        return self.active_streams != 0

    def _incr_active_streams(self):
        self.active_streams += 1

    def _decr_active_streams(self):
        self.active_streams -= 1

class DcpClient(object):

    def __init__(self, priority="medium"):
        self.lock = threading.Lock()
        self.rest = None
        self.connection = None
        self.priority = priority

    # Returns true is connections are successful
    def connect(self, host, port, bucket, user, pwd, handler):
        # Runs open connection on each node and sends control commands
        self.lock.acquire()
        if self.connection is not None:
            raise ConnectedException("Connection already established")

        self.rest = RestClient(host, port, user, pwd)
        self.rest.update()
        cluster_config = self.rest.get_nodes()
        bucket_config = self.rest.get_bucket(bucket)
        bucket_password = bucket_config['password'].encode('ascii')

        self.connection = ConnectionManager(handler)
        self.connection.connect(cluster_config, bucket_config)

        # Send the sasl auth message
        latch = CountdownLatch(len(self.rest.get_nodes()))
        op = SaslPlain(bucket, bucket_password, latch)
        self.connection.add_operation_all(op)
        # Todo: Check the value of get_result

        # Send the open connection message
        latch = CountdownLatch(len(self.rest.get_nodes()))
        op = OpenConnection(FLAG_OPEN_PRODUCER, "test_stream", latch)
        self.connection.add_operation_all(op)
        # Todo: Check the value of get_result
        op.get_result()

        # Send the set priority control message
        latch = CountdownLatch(len(self.rest.get_nodes()))
        op = Control("set_priority", self.priority, latch)
        self.connection.add_operation_all(op)
        # Todo: Check the value of get_result

        # Todo: Add the ability to send control messages

        self.lock.release()

    # Returns true if the stream is successfully created
    def add_stream(self, vbucket, flags, start_seqno, end_seqno, vb_uuid,
                   snap_start, snap_end):
        self.lock.acquire()
        if self.connection is None:
            raise ConnectedException("Not connected")

        latch = CountdownLatch()
        op = StreamRequest(vbucket, flags, start_seqno, end_seqno, vb_uuid,
                           snap_start, snap_end, latch)
        self.connection.add_operation(op, vbucket)
        ret = op.get_result()

        self.lock.release()
        return ret

    # Returns true if the stream is closed successfully
    def close_stream(self):
        self.lock.acquire()
        if self.connection is None:
            raise ConnectedException("Not connected")
        raise NotImplementedError("Not impemented yet")
        self.lock.release()

    def close(self):
        self.lock.acquire()
        self.connection.close()
        self.client = None
        self.nodes = None
        self.buckets = None
        self.connection = None
        self.lock.release()

