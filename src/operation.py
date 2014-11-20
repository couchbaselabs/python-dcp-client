import binascii
import Queue
import struct

import constants as C

class ResponseHandler():
    def __init__(self):
        pass

class QueuedResponseHandler(ResponseHandler):
    def __init__(self):
        ResponseHandler.__init__(self)
        self.responses = Queue.Queue()
        self.done = False

    def add_response(self, response):
        self.responses.put(response)

    def has_response(self):
        return self.responses.qsize() > 0 or not self.done

    def next_response(self):
        if self.done and self.responses.qsize() == 0:
            return None
        return self.responses.get(True)

    def ended(self):
        self.done = True

class Operation():

    opaque_counter = 0xFFFF0000

    def __init__(self, opcode, data_type, vbucket, cas, key, value, handler):
        Operation.opaque_counter += 1
        self.opcode = opcode
        self.data_type = data_type
        self.vbucket = vbucket
        self.opaque = Operation.opaque_counter
        self.cas = cas
        self.key = key
        self.value = value
        self.handler = handler

    def add_response(self, opcode, keylen, extlen, status, cas, body):
        raise NotImplementedError("Subclass must implement abstract method")

    def network_error(self):
        self.handler.add_response({'status': C.ERR_ECLIENT})
        self.handler.ended()

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

    def __init__(self, flags, name, handler = QueuedResponseHandler()):
        Operation.__init__(self, C.CMD_OPEN, 0, 0, 0, name, '', handler)
        self.flags = flags

    def add_response(self, opcode, keylen, extlen, status, cas, body):
        assert cas == 0
        assert keylen == 0
        assert extlen == 0
        self.handler.add_response({'opcode': opcode,
                                   'status': status,
                                   'value': body})
        self.handler.ended()
        return True

    def _get_extras(self):
        return struct.pack(">II", 0, self.flags)


class AddStream(Operation):

    def __init__(self, vbucket, flags, handler = QueuedResponseHandler()):
        Operation.__init__(self, C.CMD_ADD_STREAM, 0, vbucket, 0, '', '',
                           handler)
        self.flags = flags

    def add_response(self, opcode, keylen, extlen, status, cas, body):
        assert cas == 0
        assert keylen == 0

        opaque = None
        if extlen == 4:
            opaque = struct.unpack(">I", body[0:4])
        self.handler.add_response({'opcode': opcode,
                                   'status': status,
                                   'stream_opaque': opaque,
                                   'extlen': extlen,
                                   'value': body[4:]})
        self.handler.ended()
        return True

    def _get_extras(self):
        return struct.pack(">I", self.flags)


class CloseStream(Operation):

    def __init__(self, vbucket, handler = QueuedResponseHandler()):
        Operation.__init__(self, C.CMD_CLOSE_STREAM, 0, vbucket, 0, '', '',
                           handler)

    def add_response(self, opcode, keylen, extlen, status, cas, body):
        assert cas == 0
        assert keylen == 0
        assert extlen == 0
        self.handler.add_response({'opcode': opcode,
                                   'status': status,
                                   'value': body})
        self.handler.ended()
        return True

    def _get_extras(self):
        return ''


class GetFailoverLog(Operation):

    def __init__(self, vbucket, handler = QueuedResponseHandler()):
        Operation.__init__(self, C.CMD_GET_FAILOVER_LOG, 0, vbucket, 0, '', '',
                           handler)

    def add_response(self, opcode, keylen, extlen, status, cas, body):
        assert cas == 0
        assert keylen == 0
        assert extlen == 0

        if status == C.SUCCESS:
            assert len(body) % 16 == 0
        self.handler.add_response({'opcode': opcode,
                                   'status': status,
                                   'value': body})
        self.handler.ended()
        return True

    def _get_extras(self):
        return ''


class StreamRequest(Operation):

    def __init__(self, vb, flags, start_seqno, end_seqno, vb_uuid, snap_start,
                 snap_end, handler = QueuedResponseHandler()):
        Operation.__init__(self, C.CMD_STREAM_REQ, 0, vb, 0, '', '', handler)
        self.flags = flags
        self.start_seqno = start_seqno
        self.end_seqno = end_seqno
        self.vb_uuid = vb_uuid
        self.snap_start = snap_start
        self.snap_end = snap_end

    def add_response(self, opcode, keylen, extlen, status, cas, body):
        if opcode == C.CMD_STREAM_REQ:
            assert cas == 0
            assert keylen == 0
            assert extlen == 0

            result = {'opcode': opcode,
                      'status': status}

            if status == C.SUCCESS:
                assert (len(body) % 16) == 0
                result['failover_log'] = []

                pos = 0
                bodylen = len(body)
                while bodylen > pos:
                    vb_uuid, seqno = struct.unpack(">QQ", body[pos:pos+16])
                    result['failover_log'].append((vb_uuid, seqno))
                    pos += 16
            else:
                result['err_msg'] = body

            self.handler.add_response(result)
            if status != C.SUCCESS:
                return True
        elif opcode == C.CMD_STREAM_END:
            assert cas == 0
            assert keylen == 0
            assert extlen == 4
            flags = struct.unpack(">I", body[0:4])[0]
            self.handler.add_response({'opcode': opcode,
                                       'vbucket': status,
                                       'flags': flags})
            self.handler.ended()
            return True
        elif opcode == C.CMD_MUTATION:
            by_seqno, rev_seqno, flags, exp, lock_time, ext_meta_len, nru = \
                struct.unpack(">QQIIIHB", body[0:31])
            key = body[31:31+keylen]
            value = body[31+keylen:]
            self.handler.add_response({'opcode': opcode,
                                       'vbucket': status,
                                       'by_seqno': by_seqno,
                                       'rev_seqno': rev_seqno,
                                       'flags': flags,
                                       'expiration': exp,
                                       'lock_time': lock_time,
                                       'nru': nru,
                                       'key': key,
                                       'value': value})
        elif opcode == C.CMD_DELETION:
            by_seqno, rev_seqno, ext_meta_len = \
                struct.unpack(">QQH", body[0:18])
            key = body[18:18+keylen]
            self.handler.add_response({'opcode': opcode,
                                       'vbucket': status,
                                       'by_seqno': by_seqno,
                                       'rev_seqno': rev_seqno,
                                       'key': key})
        elif opcode == C.CMD_SNAPSHOT_MARKER:
            self.handler.add_response({'opcode': opcode,
                                       'vbucket': status})

        return False

    def _get_extras(self):
        return struct.pack(">IIQQQQQ", self.flags, 0, self.start_seqno,
                           self.end_seqno, self.vb_uuid, self.snap_start,
                           self.snap_end)


class SaslPlain(Operation):

    def __init__(self, username, password, handler = QueuedResponseHandler()):
        value = '\0'.join(['', username, password])
        Operation.__init__(self, C.CMD_SASL_AUTH, 0, 0, 0, 'PLAIN', value,
                           handler)

    def add_response(self, opcode, keylen, extlen, status, cas, body):
        self.handler.ended()
        self.handler.add_responses({'opcode': opcode,
                                    'status': status})
        return True

    def _get_extras(self):
        return ''
