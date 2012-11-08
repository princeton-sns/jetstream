#include <iostream>
#include "simple_net.h"

using namespace jetstream;
using namespace ::std;

/*
Awkward -- no good way to manage storage for the io service
SimpleNet::SimpleNet(boost::asio::ip::tcp::endpoint &e) :
  iosrv(), sock(new socket(iosrv)
{
  boost::system::error_code cli_error;
  socket.connect(e, cli_error);
}
*/

boost::shared_ptr<ControlMessage>
SimpleNet::get_ctrl_msg()
{
  boost::array<char, HEADER_LEN > buf;
  boost::system::error_code error;
  size_t len_len = sock.read_some(boost::asio::buffer(buf));

  assert(len_len == HEADER_LEN);
  u_int32_t len = ntohl( *(reinterpret_cast<u_int32_t*> (buf.data())));
  
  std::vector<char> buf2(len);
  
  size_t hb_len = sock.read_some(boost::asio::buffer(buf2));
  assert(hb_len == len);

  boost::shared_ptr<ControlMessage>  h(new ControlMessage);
  h->ParseFromArray(&buf2[0], (int) hb_len);
  return h;
}


boost::shared_ptr<DataplaneMessage>
SimpleNet::get_data_msg()
{
  boost::array<char, HEADER_LEN > buf;
  boost::system::error_code error;
  size_t len_len = sock.read_some(boost::asio::buffer(buf));

  assert(len_len == HEADER_LEN);
  u_int32_t len = ntohl( *(reinterpret_cast<u_int32_t*> (buf.data())));
  
  std::vector<char> buf2(len);
  size_t hb_len = sock.read_some(boost::asio::buffer(buf2));

  boost::shared_ptr<DataplaneMessage>  h(new DataplaneMessage);
  h->ParseFromArray(&buf2[0], (int) hb_len);
  return h;
}

boost::system::error_code
SimpleNet::send_msg(google::protobuf::MessageLite& m)
{
  boost::system::error_code err;
  size_t sz = (size_t) m.ByteSize();
  u_int32_t len_nbo = htonl (sz);
  size_t nbytes = sz + HEADER_LEN;

  u_int8_t * msg = new u_int8_t[nbytes];

  memcpy(msg, &len_nbo, HEADER_LEN);
  m.SerializeToArray((msg + HEADER_LEN), (int) sz);
  sock.send(boost::asio::buffer(msg, nbytes), 0, err);
  delete msg;
  if (err) {
//    cout << "failed to send: " << err.message() << " (" << err.value() << ")"<< endl;
    sock.close();
  }
  return err;
}
