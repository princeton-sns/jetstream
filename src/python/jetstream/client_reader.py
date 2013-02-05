import Queue
import socket
import threading
from random import randint

from generic_netinterface import sock_read_data_pb, sock_send_pb
from jetstream_types_pb2 import NodeID, DataplaneMessage

def tuple_str(tup):
  try:
    if len(tup.e) > 0:
      return str(map(element_str, tup.e))
  except ValueError:
    pass
  finally:
    return str(tup).replace('\n',' ')  

def element_str(e):
  if e.HasField('s_val'):
    return str(e.s_val)
  elif e.HasField('i_val'):
    return str(e.i_val)
  elif e.HasField('d_val'):
    return str(e.d_val)
  elif e.HasField('t_val'):
    return str(e.t_val)
  else:
    raise AttributeError

# TODO Raise exceptions when an instance's "run-once" methods are called more
# than once?
class ClientDataReader():
  """ Receive and process tuples at the end of an operator chain.

    An instance of this class presents an iterator returning tuples received
    from the operator chain it is appended to.
    Alternatively, the constructor option raw_data=True iterates through complete
    dataplane messages (which contain lists of tuples), because the groups of
    tuples can have meaning in addition, to the individual tuples' contents.

    Usage: see int_tests/client_reader_test.py
  """

  # TODO get SO_REUSEADDR or SO_LINGER to work
  portno = randint(9000, 65536)
  DoneSentinel = None

  def __init__(self, raw_data=False):
    self.HOST = 'localhost'
    self.PORT = ClientDataReader.portno
    self.raw_data = bool(raw_data)
    # allow multiple ClientDataReaders?
    ClientDataReader.portno += 1

  @staticmethod
  def bound_socket(addr, port):
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    s.bind((self.HOST, self.PORT))
    return s

  def prep_to_receive_data(self):
    """ Start collecting tuples from the wire. Return a NodeID corresponding to
        a port on this client. """
    addr = NodeID()
    addr.address = self.HOST
    addr.portno = self.PORT

    self.listen_sock = self.bound_socket(self.HOST, self.PORT)

    # only one connecting sender for now TODO this probably needs to change
    nSenders = 0
    self.listen_sock.listen(nSenders)

    # TODO should this be bounded? probably not critical.
    self.tuples = Queue.Queue()

    # spawn a thread to listen/receive tuples (the producer)
    self.receiver_thread = threading.Thread(target=self.receive_tuples)
    self.receiver_thread.daemon = True
    self.receiver_thread.start()

    return addr

  # establish connection with sender
  def accept_sender(self):
    self.conn_sock, remote_addr = self.listen_sock.accept()
    self.listen_sock.close()

  # accept and respond to a CHAIN_CONNECT
  def chain_establish(self):
    # assume there's only one edge to us
    response = sock_read_data_pb(self.conn_sock)
    if response.type == DataplaneMessage.CHAIN_CONNECT:
      self.respond_ready()
    else:
      raise Exception("First received message was not CHAIN_CONNECT")

  def respond_ready(self):
    response = DataplaneMessage()
    response.type = DataplaneMessage.CHAIN_READY
    sock_send_pb(self.conn_sock, response)

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
  
        # default is to map to each tuple individually
        if not self.raw_data:
          map(self.tuples.put, mesg.data)
        else:
          self.tuples.put(mesg.data)

      elif mesg.type == DataplaneMessage.NO_MORE_DATA:
        self.tuples.put(ClientDataReader.DoneSentinel)
        break
      else:
        raise ValueError('Unexpected message type')

    self.conn_sock.close()

  def __iter__(self):
    while True:
      item = self.tuples.get(block=True)
      if item is ClientDataReader.DoneSentinel:
        break
      yield item

    self.finish()

  def finish(self):
    self.conn_sock.close()
    if self.receiver_thread.isAlive():
      self.receiver_thread.join()

  # Deprecated; using this class as an iterator is more flexible.
  def blocking_read(self, callback):
    """ Give every received tuple to a callback function. """
    vals = map(callback, self)
    print '%d tuples received in blocking_read' % len(vals)
    self.finish()
