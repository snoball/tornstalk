#!/usr/bin/env python

import socket
import StringIO
from tornado.ioloop import IOLoop
from tornado.iostream import IOStream


class TornStalkError(Exception):
    """
    generic exception
    """


class TornStalkResponse(object):
    """
    as close as python can get to a struct
    """
    __slots__ = ('result', 'msg', 'data')
    def __init__(self, result=True, msg=None, data=None):
        self.result = result
        self.msg = msg
        self.data = data


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
            OUT_OF_MEMORY = self.fail,
            INTERNAL_ERROR = self.fail,
            DRAINING = self.fail,
            BAD_FORMAT = self.fail,
            UNKNOWN_COMMAND = self.fail,
            # put <pri> <delay> <ttr> <bytes>
            INSERTED = self.ret_inserted,
            BURIED = self.ret_inserted,
            EXPECTED_CRLF = self.fail,
            JOB_TOO_BIG = self.fail,
            # use
            USING = None,
            # reserve
            RESERVED = self.ret_reserved,
            DEADLINE_SOON = None,
            TIMED_OUT = None,
            # delete <id>
            DELETED = None,
            NOT_FOUND = None,
            # touch <id>
            TOUCHED = None,
            # watch <tube>
            WATCHING = None,
            #ignore <tube>
            NOT_IGNORED = None,
            )

        # open a connection to the beanstalkd
        _sock = socket.socket(
                        socket.AF_INET,
                        socket.SOCK_STREAM,
                        socket.IPPROTO_TCP
                        )
        _sock.connect((host, port))
        _sock.setblocking(False)
        self.stream = IOStream(_sock, io_loop=self._ioloop)

        # i like a placeholder for this. we'll assign it later
        self.callback = None
        self.tsr = TornStalkResponse()

    def _parse_response(self, resp):
        print "parse_response"
        tokens = resp.strip().split()
        if not tokens: return
        print 'tok:', tokens[1:]
        self._beanstalk_protocol_1x.get(tokens[0])(tokens)

    def _payload_rcvd(self, payload):
        self.tsr.data = payload[:-2] # lose the \r\n
        self.callback(self.tsr) # lose the \r\n

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
        self._command(payload)

    def cmd_reserve(self, callback):
        self.callback = callback
        cmd = 'reserve\r\n'
        self._command(cmd)

    def ret_inserted(self, toks):
        """ handles both INSERTED and BURIED """
        jobid = int(toks[1])
        self.callback(TornStalkResponse(data=jobid))

    def ret_reserved(self, toks):
        jobid, size = toks[1:]
        jobid = int(jobid)
        size = int(size) + 2 # len('\r\n')
        self.stream.read_bytes(size, self._payload_rcvd)

    def handle_error(self, *a):
        print "error", a
        raise TornStalkError(a)

    def ok(self, *a):
        print "ok", a
        return True

    def fail(self, toks):
        self.callback(TornStalkResponse(result=False, msg=toks[1]))


class Job(object):
    """
    each instance of communication starts with some type of job.

    Each Job has one connection to the beanstalk instance.
    Connections should NOT BE SHARED.
    """
    def __init__(self, settings, **kwa):
        try:
            host = settings['tornstalk_host']
            port = settings['tornstalk_port']
            assert host and int(port)
        except:
            raise TornStalkError(
                "ERROR: couldn't find tornstalk_host or tornstalk_port "
                " in your tornado settings"
                )
        self._connection = Connection(host, port, **kwa)

    def put(self, body, callback, tube='default'):
        self._connection.cmd_put(body, callback)

    def reserve(self, callback):
        self._connection.cmd_reserve(callback)

    @classmethod
    def use(self, callback):
        pass

    def delete(self, callback):
        pass

