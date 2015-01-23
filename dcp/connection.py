import logging
import select
import socket
import struct
import threading
import time

from constants import (HEADER_LEN, PKT_HEADER_FMT, CMD_OPEN, CMD_STREAM_REQ,
                       CMD_MUTATION, CMD_DELETION, CMD_SASL_AUTH,
                       CMD_SNAPSHOT_MARKER, CMD_STREAM_END, RES_MAGIC, SUCCESS)

class ConnectionManager(threading.Thread):

    def __init__(self, handler):
        threading.Thread.__init__(self)
        self.handler = handler
        self.cluster_config = None
        self.bucket_config = None

        self.connections = list()
        self.readers = list()
        self.writers = list()

        self.daemon = True
        self.running = True
        self.start()

    def connect(self, cluster_config, bucket_config):
        self.cluster_config = cluster_config
        self.bucket_config = bucket_config
        for name, node in cluster_config.items():
            conn = DcpConnection(node['host'], node['data_port'], self.handler)
            conn.connect()
            self.readers.append(conn.socket)
            self.connections.append(conn)

    def add_operation(self, operation, vbucket):
        host = self.bucket_config['vbmap'][vbucket]
        conn = self._get_connection_by_host(host)

        if conn is None:
            logging.warning('Trying to send op, but cannot find connection')
        else:
            conn.write(operation)
            self.writers.append(conn.socket)
            if operation.opcode is CMD_STREAM_REQ:
                self.handler._incr_active_streams()

    def add_operation_all(self, operation):
        for connection in self.connections:
            connection.write(operation)
            self.writers.append(connection.socket)

    def run(self):
        bytes_read = ''
        while self.running:
            r_ready, w_ready, errors = select.select(self.readers,
                                                     self.writers,
                                                     [], .25) # Add better timeout
            
            for reader in r_ready:
                data = reader.recv(1024)

                conn = self._get_connection_by_socket(reader)

                if conn is None:
                    logging.warn('Read response, but can\'t find a connection')
                    self.readers.remove(reader)

                if len(data) == 0:
                    logging .info('Connection lost')
                    #logger.info("Connection lost for %s:%d", conn.host, conn.port)
                    #self._connection_lost()
                else:
                    conn.bytes_read(data)

            for writer in w_ready:
                conn = self._get_connection_by_socket(writer)
                if conn is None:
                    logging.warn('Cannot write response, no connection')
                else:
                    conn.socket_write()
                self.writers.remove(writer)

    def close(self):
        self.running = False
        self.join()
        for conn in self.connections:
            conn.close()
        self.connections = None
        self.readers = list()
        self.writers = list()

    def _get_connection_by_host(self, host):
        for conn in self.connections:
            if conn.compare_by_host(host):
                return conn
        return None

    def _get_connection_by_socket(self, socket):
        for conn in self.connections:
            if conn.compare_by_socket(socket):
                return conn
        return None

class DcpConnection(object):

    def __init__(self, host, port, handler):
        self.host = host
        self.port = port
        self.handler = handler
        self.toRead = ''
        self.toWrite = ''
        self.socket = None
        self.writeLock = threading.Lock()
        self.ops = list()

    def connect(self):
        try:
            self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.socket.connect((self.host, self.port))
        except Exception, e:
            self.socket = None

    def compare_by_host(self, hostname):
        name = self.host + ':' + str(self.port)
        return name == hostname

    def compare_by_socket(self, socket):
        return self.socket == socket

    def bytes_read(self, bytes):
        self.toRead += bytes

        while len(self.toRead) >= HEADER_LEN:
            magic, opcode, keylen, extlen, dt, status, bodylen, opaque, cas = \
                struct.unpack(PKT_HEADER_FMT, self.toRead[0:HEADER_LEN])
            
            if len(self.toRead) < (HEADER_LEN + bodylen):
                break
                
            body = self.toRead[HEADER_LEN:HEADER_LEN + bodylen]
            packet = self.toRead[0:HEADER_LEN + bodylen]
            self.toRead = self.toRead[HEADER_LEN+bodylen:]

            if opcode in [CMD_OPEN, CMD_STREAM_REQ, CMD_SASL_AUTH]:
                if opcode is CMD_STREAM_REQ and status is not SUCCESS:
                    self.handler._decr_active_streams()

                for oper in self.ops:
                    if oper.opaque == opaque:
                        oper.add_response(opcode, keylen, extlen,
                                          status, cas, body)
            elif opcode is CMD_MUTATION:
                self._handle_mutation(keylen, extlen, status, cas, body)
            elif opcode is CMD_DELETION:
                self._handle_deletion(keylen, extlen, status, cas, body)
            elif opcode is CMD_SNAPSHOT_MARKER:
                self._handle_marker(extlen, status, body)
            elif opcode is CMD_STREAM_END:
                self._handle_stream_end(extlen, status, body)
            else:
                logging.warn('Unknown Op: %d %d' % (opcode, status))

    def write(self, op):
        self.writeLock.acquire()
        regSocket = len(self.toWrite) == 0
        self.toWrite += op.bytes()
        self.ops.append(op)
        self.writeLock.release()
        return regSocket

    def socket_write(self):
        self.writeLock.acquire()
        # Assume we can always write since writes are rare
        # This may need to be fixed in the future
        self.socket.send(self.toWrite)
        self.toWrite = ''
        self.writeLock.release()

    def close(self):
        self.socket.close()

    def _handle_mutation(self, keylen, extlen, status, cas, body):
        assert extlen == 31
        by_seqno, rev_seqno, flags, exp, lock_time, ext_meta_len, nru = \
            struct.unpack(">QQIIIHB", body[0:extlen])
        key = body[extlen:extlen+keylen]
        value = body[extlen+keylen:]
        self.handler.mutation({'vbucket': status,
                               'by_seqno': by_seqno,
                               'rev_seqno': rev_seqno,
                               'flags': flags,
                               'expiration': exp,
                               'lock_time': lock_time,
                               'nru': nru,
                               'key': key})#,
                               #'value': value})

    def _handle_deletion(self, keylen, extlen, status, cas, body):
        assert extlen == 18
        by_seqno, rev_seqno, ext_meta_len = \
            struct.unpack(">QQH", body[0:extlen])
        key = body[extlen:extlen+keylen]
        self.handler.deletion({'vbucket': status,
                               'by_seqno': by_seqno,
                               'rev_seqno': rev_seqno,
                               'key': key})

    def _handle_marker(self, extlen, status, body):
        assert extlen == 20
        snap_start, snap_end, snap_type = \
            struct.unpack(">QQI", body[0:extlen])
        self.handler.marker({'vbucket': status,
                             'snap_start': snap_start,
                             'snap_end': snap_end,
                             'snap_type': snap_type})

    def _handle_stream_end(self, extlen, status, body):
        assert extlen == 4
        flags = struct.unpack(">I", body[0:extlen])[0]
        self.handler.stream_end({'vbucket': status,
                                 'flags': flags})
        self.handler._decr_active_streams()
