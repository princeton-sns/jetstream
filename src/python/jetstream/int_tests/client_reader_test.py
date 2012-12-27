import unittest
import time
import os
import signal
from tempfile import mkstemp
import itertools

from jetstream_types_pb2 import *
from controller import *
import query_graph as jsapi

from client_reader import ClientDataReader

from generic_netinterface import JSClient


class TestClientReaderIntegration(unittest.TestCase):

  def setUp(self):
    self.controller = Controller(('localhost', 0))
    self.controller.start()
    print "controller bound to %s:%d" % self.controller.address

    self.client = JSClient(self.controller.address)

  def tearDown(self):
    self.controller.stop()
    self.client.close()
    os.close(self.jsnode_out[0])
    os.killpg(self.workerProc.pid, signal.SIGKILL)

  # returns the response to this request
  def make_deploy_request(self, graph):
    # copy graph to alter message
    req = ControlMessage()
    req.type = ControlMessage.ALTER
    req.alter.Clear()

    graph.add_to_PB(req.alter)

    return self.client.do_rpc(req, True)

  def validate_response(self, buf):
    resp = ControlMessage()
    resp.ParseFromString(buf)

    if resp.type == ControlMessage.ERROR:
        print "error from client", resp.error_msg
    self.assertEquals(resp.type, ControlMessage.OK)
    # Make sure the controller created state for this computation
    self.assertEquals(len(self.controller.computations), 1)

    # Wait for the topology to start running on the worker
    time.sleep(1)

  def make_local_worker(self):

    try:
      self.workerProc.terminate()
    except AttributeError:
        pass

    jsnode_cmd = "./jsnoded -a %s:%d --start " \
                 "-C ./config/datanode.conf &"
    jsnode_cmd = jsnode_cmd % self.controller.address
    print "starting", jsnode_cmd
    self.jsnode_out = mkstemp(suffix='client_reader_test_jsnode_dump',
                              prefix='/tmp/jsnode')
    self.workerProc = subprocess.Popen(jsnode_cmd, shell=True,
                                       preexec_fn=os.setsid,
                                       stdout=self.jsnode_out[0],
                                       stderr=subprocess.STDOUT)
    time.sleep(2)

    # shouldn't have terminated yet
    # TODO why does this fail
    #self.assertIsNone(self.workerProc.poll())

  def test_reader(self):
    g = jsapi.QueryGraph()

    k = 40
    echoer = jsapi.SendK(g, k)

    resultReader = ClientDataReader()
    g.connectExternal(echoer, resultReader.prep_to_receive_data())

    self.make_local_worker()
    #self.controller.deploy(g)
    self.validate_response(self.make_deploy_request(g))

    # validate SendK by counting
    tuplesReceived = []
    resultReader.blocking_read(lambda x: tuplesReceived.append(x))

    self.assertEquals(len(tuplesReceived), k)


if __name__ == '__main__':
  unittest.main()
  sys.exit(0)
