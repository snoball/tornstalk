
import unittest

from random import randint

from .beanstalk_util import BeanstalkTest

from ..tornstalk import Job

class TornStalkTests(BeanstalkTest):

    def test_connection(self):
        job = Job(
                'localhost',
                self.beanstalk_port,
                io_loop = self.io_loop.instance()
                )
        job.put('xxxx', self.stop)
        print "after job put"
        x = self.wait()
        print x
