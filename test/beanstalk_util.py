
from random import randint
import subprocess
import time

import nose.tools
from tornado.web import _O
from tornado.testing import AsyncTestCase

# influenced/inspired by trombi's with_couchdb decorator

class BeanstalkTest(AsyncTestCase):

    def setUp(self):
        AsyncTestCase.setUp(self)
        self.beanstalk_port = 6480
        """
        randint(8099,9099)
        cmdline = "/usr/bin/env beanstalkd -p {port}".format(port=self.beanstalk_port)
        null = open('/dev/null', 'w')
        self._proc = subprocess.Popen(cmdline, shell=True, stdout=null, stderr=null)
        time.sleep(0.6) # time to startup
        print "beanstalkd running at", self._proc.pid
        """

    def tearDown(self):
        super(BeanstalkTest, self).tearDown()
        """
        self._proc.terminate()
        self._proc.wait()
        """

