
import subprocess
import time
import unittest
from random import randint

import nose.tools
from tornado.web import _O

# influenced/inspired by trombi's with_couchdb decorator

class BeanstalkTest(unittest.TestCase):

    def setUp(self):
        super(BeanstalkTest, self).setUp()
        self.beanstalk_port = randint(8099,9099)
        cmdline = "/usr/bin/env beanstalkd -p {port}".format(port=self.beanstalk_port)
        null = open('/dev/null', 'w')
        self._proc = subprocess.Popen(cmdline, shell=True, stdout=null, stderr=null)
        time.sleep(0.6) # time to startup
        print "beanstalkd running at", self._proc.pid

    def tearDown(self):
        super(BeanstalkTest, self).tearDown()
        self._proc.terminate()
        self._proc.wait()

