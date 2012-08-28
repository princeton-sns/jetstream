
#include "simple_net.h"
#include <iostream>

using namespace jetstream;

boost::shared_ptr<ControlMessage>
SimpleNet::get_ctrl_msg()
{
  boost::array<char, HEADER_LEN > buf;
  boost::system::error_code error;
  int len_len = sock.read_some(boost::asio::buffer(buf));

  assert(len_len == HEADER_LEN);
  int32_t len = ntohl( *(reinterpret_cast<int32_t*> (buf.data())));
  
  std::vector<char> buf2(len);
  int hb_len = sock.read_some(boost::asio::buffer(buf2));


  boost::shared_ptr<ControlMessage>  h(new ControlMessage);
  h->ParseFromArray(&buf2[0], hb_len);
  return h;
}


boost::shared_ptr<DataplaneMessage>
SimpleNet::get_data_msg()
{
  boost::array<char, HEADER_LEN > buf;
  boost::system::error_code error;
  int len_len = sock.read_some(boost::asio::buffer(buf));

  assert(len_len == HEADER_LEN);
  int32_t len = ntohl( *(reinterpret_cast<int32_t*> (buf.data())));
  
  std::vector<char> buf2(len);
  int hb_len = sock.read_some(boost::asio::buffer(buf2));

  boost::shared_ptr<DataplaneMessage>  h(new DataplaneMessage);
  h->ParseFromArray(&buf2[0], hb_len);
  return h;
}

void
SimpleNet::send_msg(google::protobuf::MessageLite& m)
{
  int sz = m.ByteSize();
  u_int32_t len_nbo = htonl (sz);
  int nbytes = sz + HEADER_LEN;

  u_int8_t * msg = new u_int8_t[nbytes];

  memcpy(msg, &len_nbo, HEADER_LEN);
  m.SerializeToArray((msg + HEADER_LEN), sz);

//  std::cout << "synchronous send. First bytes of mock data: " <<
//     ((int*) msg)[1] << std::endl;

  
  sock.send(boost::asio::buffer(msg, nbytes));
  delete msg;
}