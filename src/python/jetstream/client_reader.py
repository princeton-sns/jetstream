# has a blocking dequeue() TODO is this the right choice?
from Queue import Queue
import socket
import threading
from random import randint

from generic_netinterface import sock_read_data_pb, sock_send_pb
from jetstream_types_pb2 import NodeID, DataplaneMessage


# client reader instance only cleans up during blocking_read(), so right now
# blocking_read() is the only way to call it without leaving a mess of
# sockets/threads.
# TODO Raise exceptions when an instance's "run-once" methods are called more
# than once
class ClientDataReader():
  """ Receive and process tuples sent over the network.

    Usage: see int_tests/client_reader_test.py

    g = jsapi.QueryGraph()

    k = 40
    echoer = jsapi.SendK(g, k)

    resultReader = ClientDataReader()
    # as it stands, this adds an edge from an operator to this bare
    # node, which specifies that tuples should come here.
    selfNodeID = resultReader.prep_to_receive_data()
    g.connectExternal(echoer, selfNodeID)

    self.make_local_worker()
    #self.controller.deploy(g)
    self.validate_response(self.make_deploy_request(g))

    # validate SendK by counting
    tuplesReceived = []
    resultReader.blocking_read(lambda x: tuplesReceived.append(x))

    self.assertEquals(len(tuplesReceived), k)
  """

  # TODO get SO_REUSEADDR or SO_LINGER to work
  portno = randint(9000, 65536)
  DoneSentinel = None

  def __init__(self, sync=False):
    self.HOST = 'localhost'
    self.PORT = ClientDataReader.portno
    # allow multiple ClientDataReaders?
    ClientDataReader.portno += 1

  def prep_to_receive_data(self):
    """ Start collecting tuples from the wire. Return a NodeID corresponding to
        a port on this client. """
    addr = NodeID()
    addr.address = self.HOST
    addr.portno = self.PORT

    self.listen_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    self.listen_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    self.listen_sock.bind((self.HOST, self.PORT))

    # only one connecting sender for now TODO this probably needs to change
    nsenders = 0
    self.listen_sock.listen(nsenders)

    # TODO should this be bounded? probably not critical.
    self.tuples = Queue()

    # spawn a thread to listen/receive tuples (the producer)
    self.receiver_thread = threading.Thread(target=self.receive_tuples)
    self.receiver_thread.start()

    return addr

  # establish connection with sender
  def accept_sender(self):
    self.conn_sock, remote_addr = self.listen_sock.accept()

  # accept and respond to a CHAIN_CONNECT
  def chain_establish(self):
    # assume there's only one edge to us
    response = sock_read_data_pb(self.conn_sock)
    if response.type == DataplaneMessage.CHAIN_CONNECT:
      self.respond_ready()
    else:
      raise Exception("First received message was not CHAIN_CONNECT")

  # get stream of tuples and stick them in a list: method executed by producer
  # thread.
  def receive_tuples(self):
    self.accept_sender()
    self.chain_establish()

    while 1:
      mesg = sock_read_data_pb(self.conn_sock)
      if mesg.type == DataplaneMessage.DATA:

        # TODO is this redundant?
        assert mesg.data is not ClientDataReader.DoneSentinel
        assert mesg.data is not None

        # map to each tuple individually
        map(self.tuples.put, mesg.data)

      elif mesg.type == DataplaneMessage.NO_MORE_DATA:
        # TODO add option: hand more information back to the caller: entire
        # dataplane message?
        self.tuples.put(ClientDataReader.DoneSentinel)
        break

      else:
        raise Exception('Unexpected message type')

    self.conn_sock.close()

  def respond_ready(self):
    response = DataplaneMessage()
    response.type = DataplaneMessage.CHAIN_READY
    sock_send_pb(self.conn_sock, response)

  def tuple_consumer(self):
    while True:
      item = self.tuples.get(block=True)
      if item is ClientDataReader.DoneSentinel:
        break
      yield item
    # TODO does this work? probably need to reread threads chapter of OS text

  def consume(self, cb):
    return map(cb, self.tuple_consumer())

  def finish(self):
    self.listen_sock.close()
    if self.receiver_thread.isAlive():
      self.receiver_thread.join()

  def blocking_read(self, callback):
    """ Give every received tuple to a callback function. """
    self.finish()
    vals = self.consume(callback)
    print '%d tuples received in blocking_read' % len(vals)
