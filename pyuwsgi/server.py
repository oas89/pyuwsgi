import os
import sys
import time
import errno
import select
import signal
import logging

from . import errors, util

logger = logging.getLogger(__name__)


class Server(object):
    mercy = 20

    def __init__(self, sock, app,
                 timeout=1,
                 procnum=1,
                 worker_cls=None,
                 connection_cls=None,
                 handler_cls=None,
                 max_requests=None,
                 max_lifetime=None):
        self.sock = sock
        self.app = app
        self.timeout = timeout
        self.procnum = procnum
        self.worker_cls = worker_cls
        self.connection_cls = connection_cls
        self.handler_cls = handler_cls
        self.max_requests = max_requests
        self.max_lifetime = max_lifetime

        self.workers = []

        signal.signal(signal.SIGQUIT, lambda n, f: self.stop())
        signal.signal(signal.SIGTERM, lambda n, f: self.stop_gracefully())

        self._is_stopping = False
        self._is_stopping_gracefully = False

    @property
    def is_stopping(self):
        return self._is_stopping or self._is_stopping_gracefully

    def stop(self):
        logger.info('[master] (pid %s) SIGQUIT received, stopping workers...', os.getpid())
        self._is_stopping = True
        for worker in self.workers:
            worker.death = time.time() + self.mercy
            util.kill(worker.pid, signal.SIGQUIT)

    def stop_gracefully(self):
        logger.info('[master] (pid %s) SIGTERM received, gracefully stopping workers...', os.getpid())
        self._is_stopping_gracefully = True
        for worker in self.workers:
            worker.death = time.time() + self.mercy
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

        worker.pid = pid
        worker.birth = time.time()

        if pid:
            logger.info('[master] (pid %s) spawning worker (pid %s)', os.getpid(), pid)
            return worker

        return_code = errors.EXIT_CODE_STOP

        try:
            worker.run()
        except SystemExit:
            pass
        except errors.ApplicationError:
            logger.exception('[worker] (pid %s) cannot load application', worker.pid)
            return_code = errors.EXIT_CODE_APPLICATION_ERROR
        except:
            logger.exception('[worker] (pid %s) unhandled exception', worker.pid)
            return_code = errors.EXIT_CODE_UNHANDLED_EXCEPTION

        logger.info('[worker] (pid %s) stopped', worker.pid)
        os._exit(return_code)

    def run(self):
        pid = os.getpid()

        logger.info('[master] (pid %s) running', pid)

        for i in range(self.procnum):
            worker = self.spawn()
            self.workers.append(worker)

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
        except KeyboardInterrupt:
            self.stop()
        except:
            logger.exception('[master] (pid %s) unhandled exception', os.getpid())
            for worker in self.workers:
                util.kill(worker.pid, signal.SIGKILL)
            sys.exit(errors.EXIT_CODE_UNHANDLED_EXCEPTION)
        finally:
            logger.info('[master] (pid %s) stopped', os.getpid())

    def check_state(self):
        if self.is_stopping and not len(self.workers):
            raise SystemExit(errors.EXIT_CODE_STOP)

        if len(self.workers) < self.procnum:
            worker = self.spawn()
            self.workers.append(worker)

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
        now = time.time()
        for worker in self.workers:
            if worker.death and worker.death < now:
                logger.info('[worker] (pid %s) death time', worker.pid)
                util.kill(worker.pid, signal.SIGKILL)
                continue

            if self.max_requests and worker.requests > self.max_requests:
                if not worker.death:
                    logger.info('[worker] (pid %s) requests limit', worker.pid)
                    worker.death = time.time() + self.mercy
                    util.kill(worker.pid, signal.SIGTERM)
                    continue

            if self.max_lifetime and worker.lifetime > self.max_lifetime:
                if not worker.death:
                    logger.info('[worker] (pid %s) lifetime limit', worker.pid)
                    worker.death = time.time() + self.mercy
                    util.kill(worker.pid, signal.SIGTERM)
                    continue
