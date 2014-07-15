import os
import time
import mmap
import errno
import ctypes
import signal
import socket
import logging
from . import util, errors

logger = logging.getLogger(__name__)


class Worker(object):

    def __init__(self, sock, app, timeout=1, connection_cls=None,
                 handler_cls=None):
        self.sock = sock
        self.app = app
        self.timeout = timeout
        self.connection_cls = connection_cls
        self.handler_cls = handler_cls

        self.pid = 0
        self.birth = 0
        self.death = 0

        self._shared = mmap.mmap(-1, mmap.PAGESIZE)
        self._requests = ctypes.c_int.from_buffer(self._shared, 1)
        self._accepting = ctypes.c_bool.from_buffer(self._shared, False)

    @property
    def requests(self):
        return self._requests.value

    @requests.setter
    def requests(self, value):
        self._requests.value = value

    @property
    def accepting(self):
        return self._accepting.value

    @accepting.setter
    def accepting(self, value):
        self._accepting.value = value

    def stop(self):
        raise StopIteration

    def stop_gracefully(self):
        self.accepting = False

    def reset(self, pid):
        self.pid = pid or os.getpid()
        self.birth = time.time()
        self.death = 0
        self.requests = 0
        self.accepting = False

    def run(self):
        signal.signal(signal.SIGQUIT, lambda n, f: self.stop())
        signal.signal(signal.SIGTERM, lambda n, f: self.stop_gracefully())

        util.seed()
        util.set_blocking(self.sock)

        self.app = util.import_name(self.app)
        self.accepting = True

        logger.info('[worker] (pid %s) accepting connections', self.pid)

        while self.accepting:
            try:
                client, addr = self.sock.accept()
            except socket.error as e:
                if e.args[0] in [errno.EINTR, errno.EAGAIN]:
                    continue
                raise

            self.handle(client, addr)
            self.requests += 1

    def handle(self, client, addr):
        with self.connection_cls(client, self.app) as connection:
            logger.debug(
                '[worker] (pid %s) %s %s "%s"',
                self.pid,
                addr[0],
                connection.environ['REQUEST_METHOD'],
                connection.environ['REQUEST_URI'],
            )
            handler = self.handler_cls(
                connection.stdin,
                connection.stdout,
                connection.stderr,
                connection.environ,
                multithread=False,
                multiprocess=True,
            )
            handler.run(self.app)
