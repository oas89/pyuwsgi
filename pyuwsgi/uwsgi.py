import struct
import logging
from . import util


logger = logging.getLogger(__name__)

HEADER_SIZE = 4
MODIFIER1 = 0
MODIFIER2 = 0


def unpack_header(data):
    return struct.unpack('<BHB', data)


def unpack_pair(data, pos=0):
    key_length = struct.unpack('<H', data[pos:pos+2])[0]
    pos += 2
    key = data[pos:pos+key_length]
    pos += key_length
    value_length = struct.unpack('<H', data[pos:pos+2])[0]
    pos += 2
    value = data[pos:pos+value_length]
    pos += value_length
    return pos, (key, value)


def unpack_pairs(data):
    pos, end = 0, len(data)
    while pos < end:
        pos, (key, value) = unpack_pair(data, pos)
        yield key, value


class Connection(object):

    def __init__(self, sock, app):
        util.set_blocking(sock)  # XXX: Not sure about this
        self.sock = sock
        self.app = app
        self.stdin = self.sock.makefile('r', 4096)  # XXX: Is it a right way to
        self.stdout = self.sock.makefile('w')       # handle socket I/O?
        self.stderr = self.stdout
        self.environ = {}

    def begin(self):
        data = self.stdin.read(HEADER_SIZE)

        if len(data) != HEADER_SIZE:
            raise IOError

        modifier1, size, modifier2 = unpack_header(data)

        if (modifier1, modifier2) != (MODIFIER1, MODIFIER2):
            raise IOError

        if size:
            data = self.stdin.read(size)

            if len(data) != size:
                raise IOError

            pairs = unpack_pairs(data)

            self.environ.update(pairs)


    def close(self):
        util.close(self.stdin)
        util.close(self.stdout)
        util.close(self.sock)

    def __enter__(self):
        self.begin()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
