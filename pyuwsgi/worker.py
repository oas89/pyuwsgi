import os
import sys
import errno
import signal
import select
import socket
import logging
from . import util, errors


logger = logging.getLogger(__name__)


class Worker(object):

    pid = util.shared_property('pid', 'L', initial=0, readonly=False)
    requests = util.shared_property('requests', 'L', initial=0, readonly=False)
    managing_next_request = util.shared_property('managing_next_request', 'L', initial=0, readonly=False)

    def __init__(self, sock, app, timeout=1, connection_cls=None,
                 handler_cls=None):
        self.sock = sock
        self.app = app
        self.timeout = timeout
        self.connection_cls = connection_cls
        self.handler_cls = handler_cls
        self.deathtime = None

    def stop(self):
        sys.exit(errors.EXIT_CODE_STOP)

    def stop_gracefully(self):
        self.managing_next_request = False

    def run(self):
        signal.signal(signal.SIGINT, lambda n, f: self.stop())
        signal.signal(signal.SIGTERM, lambda n, f: self.stop_gracefully())

        util.seed()
        util.set_blocking(self.sock)

        self.app = util.import_name(self.app)
        self.pid = os.getpid()
        self.managing_next_request = True

        while self.managing_next_request:
            try:
                select.select([self.sock], [], [], self.timeout)
            except select.error as e:
                if e.args[0] in [errno.EINTR, errno.EAGAIN]:
                    continue
                raise

            try:
                client, addr = self.sock.accept()
            except socket.error as e:
                if e.args[0] in [errno.EINTR, errno.EAGAIN]:
                    continue
                raise

            try:
                self.handle(client, addr)
                self.requests += 1
            except Exception as e:
                logger.exception(
                    '[Worker pid %s] Unhandled exception', self.pid)

    def handle(self, client, addr):
        with self.connection_cls(client, self.app) as connection:
            logger.debug(
                '[Worker pid %s] Handling request from %s "%s"',
                self.pid, addr[0], connection.environ['PATH_INFO'])
            handler = self.handler_cls(
                connection.stdin,
                connection.stdout,
                connection.stderr,
                connection.environ,
                multithread=False,
                multiprocess=True,
            )
            handler.run(self.app)
