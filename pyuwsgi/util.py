import os
import fcntl
import errno
import random
import ctypes
import ctypes.util

from .errors import ApplicationError


def set_process_title(title):
    libc = ctypes.CDLL(ctypes.util.find_library('c'))
    argc = ctypes.c_int()
    argv = ctypes.POINTER(ctypes.c_char_p)()
    ctypes.pythonapi.Py_GetArgcArgv(ctypes.byref(argc), ctypes.byref(argv))

    length = 0

    for n in range(argc.value):
        length += len(argv[n]) + 1

    title = title[:length - 2]
    title = title.ljust(length, '\x00')

    libc.strncpy(argv.contents, title, length)


def kill(pid, sig):
    try:
        os.kill(pid, sig)
    except OSError as e:
        if e.errno not in [errno.ESRCH]:
            raise


def close(fd):
    if hasattr(fd, 'close'):
        fd.close()
    else:
        os.close(fd)


def seed():
    random.seed(os.urandom(128))


def set_blocking(fd):
    if hasattr(fd, 'setblocking'):
        fd.setblocking(1)
    else:
        if hasattr(fd, 'fileno'):
            fd = fd.fileno()
        flags = fcntl.fcntl(fd, fcntl.F_GETFD) & ~os.O_NONBLOCK
        fcntl.fcntl(fd, fcntl.F_SETFL, flags)


def set_not_blocking(fd):
    if hasattr(fd, 'setblocking'):
        fd.setblocking(0)
    else:
        if hasattr(fd, 'fileno'):
            fd = fd.fileno()
        flags = fcntl.fcntl(fd, fcntl.F_GETFD) | os.O_NONBLOCK
        fcntl.fcntl(fd, fcntl.F_SETFL, flags)


def set_close_on_exec(fd):
    if hasattr(fd, 'fileno'):
        fd = fd.fileno()
    flags = fcntl.fcntl(fd, fcntl.F_GETFD) | fcntl.FD_CLOEXEC
    fcntl.fcntl(fd, fcntl.F_SETFL, flags)


def import_name(import_string):
    parts = import_string.split(':', 1)
    module_name = parts[0]
    app_name = parts[1] if len(parts) > 1 else 'application'
    try:
        module = __import__(module_name)
        app = getattr(module, app_name)
        if not callable(app):
            raise ApplicationError('WSGI-application must be callable object.')
    except ImportError:
        raise ApplicationError('Cannot import module: {}.'.format(module_name))
    except AttributeError:
        raise ApplicationError('Cannot find object: {}.'.format(app_name))
    return app


def daemonize(umask=0):
    if os.fork():
        os._exit(0)

    os.setsid()

    os.umask(umask)

    with os.open(os.devnull, os.O_RDWR) as null:
        os.dup2(null, 0)
        os.dup2(null, 1)
        os.dup2(null, 2)
