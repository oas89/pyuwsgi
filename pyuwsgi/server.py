import os
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
                 processes=1,
                 worker_cls=None,
                 connection_cls=None,
                 handler_cls=None,
                 max_requests=None,
                 max_lifetime=None):
        self.sock = sock
        self.app = app
        self.timeout = timeout
        self.processes = processes
        self.worker_cls = worker_cls
        self.connection_cls = connection_cls
        self.handler_cls = handler_cls
        self.max_requests = max_requests
        self.max_lifetime = max_lifetime

        self.workers = {}

        self.is_stopping = False
        self.chaining = None
        self.chaining_pid = None

        self._signals = []
        self._selfpipe = []

    def find_worker_id(self, pid):
        for n, worker in self.workers.items():
            if worker.pid == pid:
                return n

    def stop(self):
        logger.info('stopping workers...')
        self.is_stopping = True
        for worker in self.workers.values():
            if worker.pid > 0 and not worker.death:
                worker.death = time.time() + self.mercy
                util.kill(worker.pid, signal.SIGQUIT)

    def stop_gracefully(self):
        logger.info('stopping workers gracefully...')
        self.is_stopping = True
        for worker in self.workers.values():
            if worker.pid > 0:
                worker.death = time.time() + self.mercy
                util.kill(worker.pid, signal.SIGTERM)

    def reload(self):
        logger.info('reloading workers...')
        for worker in self.workers.values():
            if worker.pid > 0:
                worker.death = time.time() + self.mercy
                util.kill(worker.pid, signal.SIGTERM)

    def chain(self):
        logger.info('chaining workers...')
        self.chaining = 0

    def spawn(self, n):
        worker = self.workers[n]
        pid = os.fork()
        worker.reset(pid)

        if pid:
            logger.info('spawning worker %s pid %s', n, pid)
            return

        try:
            worker.run()
        except StopIteration:
            pass
        except errors.ApplicationError:
            os._exit(errors.APPLICATION_ERROR)
        except:
            os._exit(errors.UNHANDLED_EXCEPTION)
        os._exit(errors.STOPPING)

    def signal(self, signum, frame):
        if len(self._signals) < 10:
            self._signals.append(signum)

    def setup_signals(self):
        if self._selfpipe:
            map(os.close, self._selfpipe)

        self._selfpipe = os.pipe()

        map(util.set_not_blocking, self._selfpipe)
        #map(util.set_close_on_exec, self._signal_pipe)

        signal.set_wakeup_fd(self._selfpipe[1])

        for signame in ['SIGINT', 'SIGQUIT', 'SIGTERM', 'SIGCHLD', 'SIGWINCH', 'SIGHUP']:
            signum = getattr(signal, signame)
            signal.signal(signum, self.signal)

    def setup_workers(self):
        for n in range(self.processes):
            self.workers[n] = self.worker_cls(
                self.sock,
                self.app,
                timeout=self.timeout,
                connection_cls=self.connection_cls,
                handler_cls=self.handler_cls,
            )
            self.spawn(n)

    def run(self):
        logger.info('running server pid %s', os.getpid())

        self.setup_signals()
        self.setup_workers()

        try:
            while True:

                signum = self._signals.pop(0) if len(self._signals) else None

                if signum:
                    if signum == signal.SIGINT:
                        self.stop()
                    elif signum == signal.SIGQUIT:
                        self.stop()
                    elif signum == signal.SIGTERM:
                        self.stop_gracefully()
                    elif signum == signal.SIGWINCH:
                        self.reload()
                    elif signum == signal.SIGHUP:
                        self.chain()
                    elif signum == signal.SIGCHLD:
                        pass
                    else:
                        logger.warning('ignoring signal %s', signum)

                try:
                    select.select([self._selfpipe[0]], [], [], self.timeout)
                    while os.read(self._selfpipe[0], 1):
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
                self.check_chaining()
        except StopIteration:
            pass
        except:
            logger.exception('unhandled exception in server loop')
            for worker in self.workers.values():
                util.kill(worker.pid, signal.SIGKILL)
            os._exit(errors.UNHANDLED_EXCEPTION)
        os._exit(errors.STOPPING)

    def check_state(self):
        if self.is_stopping:
            for worker in self.workers.values():
                if worker.pid > 0:
                    return
            raise StopIteration

    def check_children(self):
        try:
            pid, status = os.waitpid(-1, os.WNOHANG)
            if pid <= 0:
                return

            wid = self.find_worker_id(pid)
            worker = self.workers.get(wid)

            if not worker:
                logger.warning('unknown process pid %s died', pid)
                return

            worker.reset(-1)

            if self.is_stopping:
                logger.info('worker %s pid %s died', wid, pid)
                return

            if os.WIFEXITED(status):
                if os.WEXITSTATUS(status) == errors.APPLICATION_ERROR:
                    logger.info(
                        'worker %s pid %s '
                        'failed to load application, respawning',
                        wid, pid)
                elif os.WEXITSTATUS(status) == errors.UNHANDLED_EXCEPTION:
                    logger.info(
                        'worker %s pid %s '
                        'got unhandled exception, respwaning',
                        wid, pid)
                elif os.WEXITSTATUS(status) == errors.STOPPING:
                    logger.info(
                        'worker %s pid %s '
                        'exited normally, respwaning', wid, pid)
                else:
                    logger.info(
                        'worker %s pid %s '
                        'exited with status %s, respwaning',
                        wid, pid, os.WEXITSTATUS(status))
            elif os.WIFSIGNALED(status):
                logger.info(
                    'worker %s pid %s '
                    'killed by signal %s, respawning',
                    wid, pid, os.WTERMSIG(status))
            else:
                logger.warning(
                    'worker %s pid %s '
                    'died for unknown reason, respwaning',
                    wid, pid)

            if not self.is_stopping:
                self.spawn(wid)

        except OSError as e:
            if e.errno not in [errno.EINTR, errno.ECHILD]:
                raise

    def check_deadlines(self):
        for wid, worker in self.workers.items():
            if worker.pid > 0:
                if worker.death and worker.death < time.time():
                    logger.info(
                        'worker %s pid %s '
                        'dying to long, killing',
                        wid, worker.pid)
                    util.kill(worker.pid, signal.SIGKILL)
                    continue

                if not worker.death and self.max_requests and \
                                worker.requests > self.max_requests:
                    logger.info(
                        'worker %s pid %s '
                        'exceeded requests limit, stopping',
                        wid, worker.pid)
                    worker.death = time.time() + self.mercy
                    util.kill(worker.pid, signal.SIGTERM)
                    continue

                if not worker.death and self.max_lifetime and \
                                worker.lifetime > self.max_lifetime:
                    logger.info(
                        'worker %s pid %s '
                        'exceeded lifetime limit, stopping',
                        wid, worker.pid)
                    worker.death = time.time() + self.mercy
                    util.kill(worker.pid, signal.SIGTERM)
                    continue

    def check_chaining(self):
        if self.chaining is not None:
            if self.chaining >= self.processes:
                self.chaining = None
                logger.info('chaining complete')
                return

            worker = self.workers[self.chaining]

            if not self.chaining_pid:
                self.chaining_pid = worker.pid

                if not worker.death:
                    worker.death = time.time() + self.mercy
                    util.kill(worker.pid, signal.SIGTERM)
                return

            if worker.pid != self.chaining_pid:
                if worker.accepting:
                    self.chaining_pid = None
                    self.chaining += 1
