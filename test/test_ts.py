
import unittest
import nose
import new

from random import randint

from .beanstalk_util import BeanstalkTest

from ..tornstalk import Job
from tornado.ioloop import IOLoop

# i had issues with using tornado.testing.AsyncTestCase
# that i'll eventually solve. it was just not the problem i wanted
# to solve today, so i'm using the trombi method
def with_ioloop(func):
    @nose.tools.make_decorator(func)
    def wrapper(*args, **kwargs):
        ioloop = IOLoop()
        # Override ioloop's _run_callback to let all exceptions through
        def run_callback(self, callback):
            callback()
        ioloop._run_callback = new.instancemethod(run_callback, ioloop)
        return func(ioloop, *args, **kwargs)
    return wrapper


class TornStalkTests(BeanstalkTest):

    @with_ioloop
    def setUp(io_loop, self):
        super(TornStalkTests, self).setUp()
        self._ioloop = io_loop

        # create a job object, faking a settings dict
        self.job = Job(
            settings = {
                'tornstalk_host': 'localhost',
                'tornstalk_port': self.beanstalk_port
                },
            io_loop = io_loop
            )

    def test_put(self):

        def cb(resp):
            jobid = resp.data
            assert jobid
            self._ioloop.stop()

        self.job.put('xxxx', cb)
        print "after job put"
        self._ioloop.start()

    def test_reserve(self):

        def cb_reserve(resp):
            payload = resp.data
            assert payload == 'abcdef'

        def cb_put(resp):
            self.job.reserve(cb_reserve)
            self._ioloop.stop()

        self.job.put('abcdef', cb_put)
        self._ioloop.start()







