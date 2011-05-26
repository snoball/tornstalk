#!/usr/bin/env python

import socket
import StringIO
from tornado.ioloop import IOLoop
from tornado.iostream import IOStream


class TornStalkError(Exception):
    """
    generic exception
    """

class Connection(object):
    """
    Encapsulates the communication, including parsing, with the beanstalkd
    """

    def __init__(self, host, port, io_loop=None):
        self._ioloop = io_loop or IOLoop.instance()

        # setup our protocol callbacks
        # beanstalkd will reply with a superset of these replies, but these
        # are the only ones we handle today.  patches gleefully accepted.
        self._beanstalk_protocol_1x = dict(
            # generic returns
            OUT_OF_MEMORY = self.handle_error,
            INTERNAL_ERROR = self.handle_error,
            DRAINING = self.handle_error,
            BAD_FORMAT = self.handle_error,
            UNKNOWN_COMMAND = self.handle_error,
            # put <pri> <delay> <ttr> <bytes>
            INSERTED = self.ok,
            BURIED = self.fail,
            EXPECTED_CRLF = self.fail,
            JOB_TOO_BIG = self.fail,
            # use
            USING = self.ok,
            # reserve
            DEADLINE_SOON = self.ok,
            TIMED_OUT = self.fail,
            RESERVED = self.ok,
            # delete <id>
            DELETED = self.ok,
            NOT_FOUND = self.fail,
            # touch <id>
            TOUCHED = self.ok,
            # watch <tube>
            WATCHING = self.ok,
            #ignore <tube>
            NOT_IGNORED = self.fail,
            )

        # open a connection to the beanstalkd
        _sock = socket.socket(
                        socket.AF_INET,
                        socket.SOCK_STREAM,
                        socket.IPPROTO_TCP
                        )
        _sock.connect((host, port))
        _sock.setblocking(False)
        self.stream = IOStream(_sock, self._ioloop)

        # i like a placeholder for this. we'll assign it later
        self.callback = None

    def _command(self, contents):
        print "sending>%s<" % contents
        self.stream.write(contents)
        self.stream.read_until('\r\n', self._parse_response)

    def cmd_put(self, body, callback, priority=10000, delay=0, ttr=1):
        """
        send the put command to the beanstalkd with a message body

        priority needs to be between 0 and 2**32. lower gets done first
        delay is number of seconds before job is available in queue
        ttr is number of seconds the job has to run by a worker

        bs: put <pri> <delay> <ttr> <bytes>
        """
        self.callback = callback
        cmd = 'put {priority} {delay} {ttr} {size}'.format(
                priority = priority,
                delay = delay,
                ttr = ttr,
                size = len(body)
                )

        payload = '{}\r\n{}\r\n'.format(cmd, body)
        print "payload=", payload
        self._command(payload)

    def _parse_response(self, resp):
        print "parse_response"
        tokens = resp.strip().split()
        if not tokens: return
        print self._beanstalk_protocol_1x.get(tokens[0])
        self.callback(tokens)

    def handle_error(self, *a):
        print "error", a
        raise TornStalkError(a)

    def ok(self, *a):
        print "ok", a
        return True

    def fail(self, *a):
        print "fail", a
        return False


class Job(object):
    """
    each instance of communication starts with some type of job.

    Each Job has one connection to the beanstalk instance.
    Connections should NOT BE SHARED.
    """
    def __init__(self, host, port, **kwa):
        self._connection = Connection(host, port, **kwa)

    def put(self, body, callback, tube='default'):
        self._connection.cmd_put(body, callback)

    def reserve(cls, connection, callback):
        callback(cls())
        pass

    @classmethod
    def use(self, callback):
        pass

    def delete(self, callback):
        pass

