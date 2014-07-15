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

        self.workers = {}

        self.is_stopping = False

        self._signal_queue = []
        self._signal_pipe = []

    def find_worker_id(self, pid):
        for n, worker in self.workers.items():
            if worker.pid == pid:
                return n

    def stop(self):
        logger.info('[master] (pid %s) stopping workers...', os.getpid())
        self.is_stopping = True
        for worker in self.workers.values():
            if worker.pid and not worker.death:
                worker.death = time.time() + self.mercy
                util.kill(worker.pid, signal.SIGQUIT)

    def stop_gracefully(self):
        logger.info('[master] (pid %s) gracefully stopping workers...', os.getpid())
        self.is_stopping = True
        for worker in self.workers.values():
            if worker.pid:
                worker.death = time.time() + self.mercy
                util.kill(worker.pid, signal.SIGTERM)

    def spawn(self, n):
        worker = self.workers[n]
        pid = os.fork()
        worker.reset(pid)

        if pid:
            logger.info('[master] (pid %s) spawning worker %s (pid %s)', os.getpid(), n, pid)
            return

        try:
            worker.run()
        except StopIteration:
            os._exit(errors.STOPPING)
        except errors.ApplicationError:
            logger.exception('[worker] (pid %s) cannot load application', worker.pid)
            os._exit(errors.APPLICATION_ERROR)
        except:
            logger.exception('[worker] (pid %s) unhandled exception', worker.pid)
            os._exit(errors.UNHANDLED_EXCEPTION)
        os._exit(errors.STOPPING)

    def signal(self, signum, frame):
        if len(self._signal_queue) < 10:
            self._signal_queue.append(signum)
            try:
                os.write(self._signal_pipe[1], '\x00')
            except IOError as e:
                if e.errno not in [errno.EAGAIN, errno.EINTR]:
                    raise

    def setup_signals(self):
        if self._signal_pipe:
            map(os.close, self._signal_pipe)

        self._signal_pipe = os.pipe()

        map(util.set_not_blocking, self._signal_pipe)
        #  map(util.set_close_on_exec, self._signal_pipe)

        for signame in ['SIGINT', 'SIGQUIT', 'SIGTERM', 'SIGCHLD']:
            signum = getattr(signal, signame)
            signal.signal(signum, self.signal)

    def setup_workers(self):
        for n in range(self.procnum):
            self.workers[n] = self.worker_cls(
                self.sock,
                self.app,
                timeout=self.timeout,
                connection_cls=self.connection_cls,
                handler_cls=self.handler_cls,
            )
            self.spawn(n)

    def run(self):
        logger.info('[master] (pid %s) running', os.getpid())

        self.setup_signals()
        self.setup_workers()

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
                    elif signum == signal.SIGCHLD:
                        pass
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
        except StopIteration:
            pass
        except:
            logger.exception('[master] (pid %s) unhandled exception', os.getpid())
            for worker in self.workers.values():
                util.kill(worker.pid, signal.SIGKILL)
            os._exit(errors.UNHANDLED_EXCEPTION)
        os._exit(errors.STOPPING)

    def check_state(self):
        if self.is_stopping:
            for worker in self.workers.values():
                if worker.pid:
                    return
            raise StopIteration

    def check_children(self):
        try:
            pid, status = os.waitpid(-1, os.WNOHANG)

            if pid > 0:
                worker_id = self.find_worker_id(pid)
                worker = self.workers.get(worker_id)

                if worker:
                    worker.pid = 0
                    logger.info('[master] (pid %s) buried worker (pid %s)', os.getpid(), pid)

                    if not self.is_stopping:
                        self.spawn(worker_id)
                else:
                    logger.warning('[master] (pid %s) buried unknown process', os.getpid())

        except OSError as e:
            if e.errno not in [errno.EINTR, errno.ECHILD]:
                raise

    def check_deadlines(self):
        for worker in self.workers.values():
            if worker.pid:
                if worker.death and worker.death < time.time():
                    logger.info('[worker] (pid %s) death time', worker.pid)
                    util.kill(worker.pid, signal.SIGKILL)
                    continue

                if not worker.death and self.max_requests and worker.requests > self.max_requests:
                    logger.info('[worker] (pid %s) requests limit', worker.pid)
                    worker.death = time.time() + self.mercy
                    util.kill(worker.pid, signal.SIGTERM)
                    continue

                if not worker.death and self.max_lifetime and worker.lifetime > self.max_lifetime:
                    logger.info('[worker] (pid %s) lifetime limit', worker.pid)
                    worker.death = time.time() + self.mercy
                    util.kill(worker.pid, signal.SIGTERM)
                    continue
