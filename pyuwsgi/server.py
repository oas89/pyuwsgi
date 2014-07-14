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

        self._is_stopping = False
        self._is_stopping_gracefully = False

        self._signal_queue = []
        self._signal_pipe = []

    @property
    def is_stopping(self):
        return self._is_stopping or self._is_stopping_gracefully

    def stop(self):
        logger.info('[master] (pid %s) stopping workers...', os.getpid())
        self._is_stopping = True
        for worker in self.workers:
            worker.death = time.time() + self.mercy
            util.kill(worker.pid, signal.SIGQUIT)

    def stop_gracefully(self):
        logger.info('[master] (pid %s) gracefully stopping workers...', os.getpid())
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

    def signal(self, signum, frame):
        if len(self._signal_queue) < 10:
            self._signal_queue.append(signum)
            try:
                os.write(self._signal_pipe[1], '\x00')
            except IOError as e:
                if e.errno not in [errno.EAGAIN, errno.EINTR]:
                    raise

    def run(self):
        logger.info('[master] (pid %s) running', os.getpid())

        self._signal_pipe = os.pipe()
        map(util.set_not_blocking, self._signal_pipe)
        # map(util.set_close_on_exec, self._signal_pipe)

        for signame in ['SIGINT', 'SIGQUIT', 'SIGTERM']:
            signum = getattr(signal, signame)
            signal.signal(signum, self.signal)

        for i in range(self.procnum):
            worker = self.spawn()
            self.workers.append(worker)

        try:
            while True:

                signum = self._signal_queue.pop(0) if len(self._signal_queue) else None

                if signum:
                    if signum == signal.SIGINT:
                        self.stop()
                    elif signum == signal.SIGQUIT:
                        self.stop()
                    elif signum == signal.SIGTERM:
                        self.stop_gracefully()
                    else:
                        logger.warning('[master] (pid %s) ignoring signal %s', os.getpid(), signum)

                try:
                    select.select([self._signal_pipe[0]], [], [], self.timeout)
                    while os.read(self._signal_pipe[0], 1):
                        pass
                except select.error as e:
                    if e.args[0] not in [errno.EINTR, errno.EAGAIN]:
                        raise
                except OSError as e:
                    if e.errno not in [errno.EINTR, errno.EAGAIN]:
                        raise

                self.check_state()
                self.check_children()
                self.check_deadlines()
        except SystemExit:
            raise
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
