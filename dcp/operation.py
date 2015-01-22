import binascii
import Queue
import struct

import constants as C

import threading

class CountdownLatch(object):
    def __init__(self, count=1):
        self.count = count
        self.lock = threading.Condition()
    
    def count_down(self):
        self.lock.acquire()
        self.count -= 1
        if self.count <= 0:
            self.lock.notifyAll()
        self.lock.release()
    
    def await(self):
        self.lock.acquire()
        while self.count > 0:
            self.lock.wait()
        self.lock.release()


class Operation():

    opaque_counter = 0xFFFF0000

    def __init__(self, opcode, data_type, vbucket, cas, key, value):
        Operation.opaque_counter += 1
        self.opcode = opcode
        self.data_type = data_type
        self.vbucket = vbucket
        self.opaque = Operation.opaque_counter
        self.cas = cas
        self.key = key
        self.value = value
        self.result = None

    def add_response(self, opcode, keylen, extlen, status, cas, body):
        raise NotImplementedError("Subclass must implement abstract method")

    def network_error(self):
        pass

    def get_result(self):
        self.latch.await()
        assert self.result is not None
        return self.result

    def bytes(self):
        extras = self._get_extras()
        bodylen = len(self.key) + len(extras) + len(self.value)
        header = struct.pack(C.PKT_HEADER_FMT, C.REQ_MAGIC, self.opcode,
                             len(self.key), len(extras), self.data_type,
                             self.vbucket, bodylen, self.opaque, self.cas)
        return header + extras + self.key + self.value

    def _get_extras(self):
        raise NotImplementedError("Subclass must implement abstract method")

    def __str__(self):
        return packet_2_str(self.bytes())


class OpenConnection(Operation):

    def __init__(self, flags, name, latch):
        Operation.__init__(self, C.CMD_OPEN, 0, 0, 0, name, '')
        self.flags = flags
        self.latch = latch
        self.result = True

    def add_response(self, opcode, keylen, extlen, status, cas, body):
        assert cas == 0
        assert keylen == 0
        assert extlen == 0

        if status != C.SUCCESS:
            self.result = False
        
        self.latch.count_down()

    def _get_extras(self):
        return struct.pack(">II", 0, self.flags)


class CloseStream(Operation):

    def __init__(self, vbucket):
        Operation.__init__(self, C.CMD_CLOSE_STREAM, 0, vbucket, 0, '', '')

    def add_response(self, opcode, keylen, extlen, status, cas, body):
        assert cas == 0
        assert keylen == 0
        assert extlen == 0
        return True

    def _get_extras(self):
        return ''


class StreamRequest(Operation):

    def __init__(self, vb, flags, start_seqno, end_seqno, vb_uuid, snap_start,
                 snap_end, latch):
        Operation.__init__(self, C.CMD_STREAM_REQ, 0, vb, 0, '', '')
        self.flags = flags
        self.start_seqno = start_seqno
        self.end_seqno = end_seqno
        self.vb_uuid = vb_uuid
        self.snap_start = snap_start
        self.snap_end = snap_end
        self.latch = latch
        self.result = dict()

    def add_response(self, opcode, keylen, extlen, status, cas, body):
        assert cas == 0
        assert keylen == 0
        assert extlen == 0

        self.result['status'] = status

        if status == C.SUCCESS:
            assert (len(body) % 16) == 0
            self.result['failover_log'] = list()

            pos = 0
            bodylen = len(body)
            while bodylen > pos:
                vb_uuid, seqno = struct.unpack(">QQ", body[pos:pos+16])
                self.result['failover_log'].append((vb_uuid, seqno))
                pos += 16
        else:
            self.result['err_msg'] = body

        self.latch.count_down()

    def _get_extras(self):
        return struct.pack(">IIQQQQQ", self.flags, 0, self.start_seqno,
                           self.end_seqno, self.vb_uuid, self.snap_start,
                           self.snap_end)


class SaslPlain(Operation):

    def __init__(self, username, password):
        value = '\0'.join(['', username, password])
        Operation.__init__(self, C.CMD_SASL_AUTH, 0, 0, 0, 'PLAIN', value)

    def add_response(self, opcode, keylen, extlen, status, cas, body):
        return True

    def _get_extras(self):
        return ''
