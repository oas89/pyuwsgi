import socket
import logging
import argparse

from wsgiref.handlers import BaseCGIHandler
from .server import Server
from .worker import Worker
from .uwsgi import Connection
from .util import parse_address


logging.basicConfig(level=logging.DEBUG)

logger = logging.getLogger(__name__)


def make_server(bind, wsgi,
                processes=1,
                backlog=socket.SOMAXCONN,
                worker_cls=Worker,
                connection_cls=Connection,
                handler_cls=BaseCGIHandler,
                max_requests=None,
                max_lifetime=None):

    family, address = parse_address(bind)
    sock = socket.socket(family, socket.SOCK_STREAM)
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    sock.bind(address)
    sock.listen(backlog)

    instance = Server(
        sock,
        wsgi,
        worker_cls=worker_cls,
        connection_cls=connection_cls,
        handler_cls=handler_cls,
        max_requests=max_requests,
        max_lifetime=max_lifetime,
        processes=processes,
    )

    return instance


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--address', '-a',
                        required=True,
                        action='store',
                        help='unix/tcp address to bind')
    parser.add_argument('--wsgi', '-w',
                        required=True,
                        action='store',
                        help='wsgi application')
    parser.add_argument('--processes', '-p',
                        action='store',
                        default=1,
                        type=int,
                        help='number of worker processes')

    args = parser.parse_args()
    server = make_server(args.address, args.wsgi, processes=args.processes)
    server.run()
