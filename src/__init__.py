import operation
from connection import Connection
from constants import FLAG_OPEN_CONSUMER, FLAG_OPEN_PRODUCER


class DcpClient():

    def __init__(self, host='127.0.0.1', port=11210):
        self.conn = Connection(host, port)
        self.conn.connect()

    def sasl_auth_plain(self, username, password):
        op = operation.SaslPlain(username, password)
        self.conn.queue_operation(op)
        return op

    def set_proxy(self, client):
        self.conn.proxy = client.conn.socket

    def open_consumer(self, *args, **kwargs):
        op = operation.OpenConnection(FLAG_OPEN_CONSUMER, *args, **kwargs)
        self.conn.queue_operation(op)
        return op

    def open_producer(self, *args, **kwargs):
        op = operation.OpenConnection(FLAG_OPEN_PRODUCER, *args, **kwargs)
        self.conn.queue_operation(op)
        return op

    def add_stream(self, *args, **kwargs):
        op = operation.AddStream(*args, **kwargs)
        self.conn.queue_operation(op)
        return op

    def close_stream(self, *args, **kwargs):
        op = operation.CloseStream(*args, **kwargs)
        self.conn.queue_operation(op)
        return op

    def get_failover_log(self, *args, **kwargs):
        op = operation.GetFailoverLog(*args, **kwargs)
        self.conn.queue_operation(op)
        return op

    def stream_req(self, *args, **kwargs):
        op = operation.StreamRequest(*args, **kwargs)
        self.conn.queue_operation(op)
        return op

    def shutdown(self):
        self.conn.close()
