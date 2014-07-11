import os
import sys
import time
import errno
import select
import signal
import logging
import datetime

from . import errors, util

logger = logging.getLogger(__name__)


class Server(object):

    max_requests = None
    max_lifetime = None

    def __init__(self, sock, app,
                 timeout=1,
                 procnum=1,
                 worker_cls=None,
                 connection_cls=None,
                 handler_cls=None):
        self.sock = sock
        self.app = app
        self.timeout = timeout
        self.procnum = procnum
        self.worker_cls = worker_cls
        self.connection_cls = connection_cls
        self.handler_cls = handler_cls

        self.workers = []

        signal.signal(signal.SIGINT, lambda n, f: self.stop())
        signal.signal(signal.SIGTERM, lambda n, f: self.stop_gracefully())

        self._is_stopping = False
        self._is_stopping_gracefully = False

    @property
    def is_stopping(self):
        return self._is_stopping or self._is_stopping_gracefully

    def stop(self):
        logger.info('SIGINT received, stopping workers...')
        self._is_stopping = True
        for worker in self.workers:
            worker.deathtime = int(time.time())
            util.kill(worker.pid, signal.SIGINT)

    def stop_gracefully(self):
        logger.info('SIGTERM received, gracefully stopping workers...')
        self._is_stopping_gracefully = True
        for worker in self.workers:
            worker.deathtime = int(time.time())
            util.kill(worker.pid, signal.SIGTERM)

    def find_worker(self, pid):
        return next((w for w in self.workers if w.pid == pid), None)

    def spawn(self):
        worker = self.worker_cls(
            self.sock,
            self.app,
            timeout=self.timeout,
            connection_cls=self.connection_cls,
            handler_cls=self.handler_cls,
        )

        pid = os.fork()

        if pid:
            logger.info('Spawning worker (pid %s)', pid)
            return worker

        try:
            worker.run()
            sys.exit(errors.EXIT_CODE_STOP)
        except SystemExit:
            raise
        except errors.ApplicationError:
            logger.exception(
                'Failed to load application in worker (pid %s)', worker.pid)
            sys.exit(errors.EXIT_CODE_APPLICATION_ERROR)
        except:
            logger.exception(
                'Unhandled exception in worker (pid %s)', worker.pid)
            sys.exit(errors.EXIT_CODE_UNHANDLED_EXCEPTION)
        finally:
            logger.info('Worker (pid %s) died', worker.pid)

    def run(self):
        logger.info('Running server (pid %s)', os.getpid())

        for i in range(self.procnum):
            self.spawn()

        try:
            while True:
                try:
                    select.select([], [], [], self.timeout)
                except select.error as e:
                    if e.args[0] not in [errno.EINTR, errno.EAGAIN]:
                        raise

                self.check_state()
                self.check_children()
                self.check_deadlines()
        except SystemExit:
            raise
        except:
            logger.exception(
                'Unhandled exception in main loop (pid %s', os.getpid())
            for worker in self.workers:
                util.kill(worker.pid, signal.SIGKILL)
            sys.exit(errors.EXIT_CODE_UNHANDLED_EXCEPTION)
        finally:
            logger.info('Master (pid %s) died', os.getpid())

    def check_state(self):
        if self.is_stopping and not len(self.workers):
            raise SystemExit(errors.EXIT_CODE_STOP)

    def check_children(self):
        try:
            pid, status = os.waitpid(-1, os.WNOHANG)
            worker = self.find_worker(pid)
            if worker:
                self.workers.remove(worker)
        except OSError as e:
            if e.errno not in [errno.EINTR, errno.ECHILD]:
                raise

    def check_deadlines(self):
        now = datetime.datetime.now()
        for worker in self.workers:
            if worker.deathtime < now:
                logger.info(
                    'Worker (pid %s) death time has come, sending SIGKILL',
                    worker.pid)
                util.kill(worker.pid, signal.SIGKILL)
                continue

            if self.max_requests and worker.requests > self.max_requests:
                logger.info(
                    'Worker (pid %s) reached a requests limit, sending SIGTERM',
                    worker.pid)
                util.kill(worker.pid, signal.SIGTERM)
                continue

            if self.max_lifetime and worker.lifetime > self.max_lifetime:
                logger.info(
                    'Worker (pid %s) reached a lifetime limit, sending SIGTERM',
                    worker.pid)
                util.kill(worker.pid, signal.SIGTERM)
                continue
