import select
import socket
import struct
import threading
import time

from dcp.constants import (HEADER_LEN, PKT_HEADER_FMT, CMD_STREAM_REQ,
                           RES_MAGIC, SUCCESS)


class Connection(threading.Thread):

    def __init__(self, host='127.0.0.1', port=11211):
        threading.Thread.__init__(self)
        self.daemon = True
        self.host = host
        self.port = port
        self.socket = None
        self.running = False
        self.ops = []
        self.proxy = None

    def connect(self):
        try:
            self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.socket.connect((self.host, self.port))
            self.running = True
            self.start()
        except Exception, e:
            self.socket = None

    def close(self, force=False):
        if not force:
            secs = 0
            while len(self.ops) > 0 and secs < 5:
                time.sleep(1)
                secs += 1
        if self.socket:
            self.running = False
            self.join()
            self.socket.close()

    def queue_operation(self, op):
        if not self.running:
            op.network_error()
            return
        for o in self.ops:
            assert op.opaque != o.opaque
        self.ops.append(op)
        self.socket.send(op.bytes())

    def run(self):
        bytes_read = ''
        rd_timeout = 1
        desc = [self.socket]
        while self.running:
            readers, writers, errors = select.select(desc, [], [], rd_timeout)
            rd_timeout = .25

            for reader in readers:
                data = reader.recv(1024)
                if len(data) == 0:
                    self._connection_lost()
                bytes_read += data

            while len(bytes_read) >= HEADER_LEN:
                magic, opcode, keylen, extlen, dt, status, bodylen, opaque, cas = \
                    struct.unpack(PKT_HEADER_FMT, bytes_read[0:HEADER_LEN])

                if len(bytes_read) < (HEADER_LEN + bodylen):
                    break

                rd_timeout = 0
                body = bytes_read[HEADER_LEN:HEADER_LEN + bodylen]
                packet = bytes_read[0:HEADER_LEN + bodylen]
                bytes_read = bytes_read[HEADER_LEN+bodylen:]

                processed = False
                for oper in self.ops:
                    if oper.opaque == opaque:
                        rm = oper.add_response(opcode, keylen, extlen,
                                               status, cas, body)
                        if rm:
                            self.ops.remove(oper)
                        processed = True
                        break

                if not processed:
                    if self.proxy is None:
                        self._handle_random_opaque(opcode, status, opaque)
                    else:
                        self.proxy.send(packet)

    def _handle_random_opaque(self, opcode, vbucket, opaque):
        if opcode == CMD_STREAM_REQ:
            resp = struct.pack(PKT_HEADER_FMT, RES_MAGIC, opcode,
                               0, 0, 0, SUCCESS, 16, opaque, 0)
            body = struct.pack("<QQ", 123456, 0)
            self.socket.send(resp + body)

    def _connection_lost(self):
        self.running = False
        for op in self.ops:
            op.network_error()
            self.ops.remove(op)
