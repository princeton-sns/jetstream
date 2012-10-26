#ifndef Jetstream_simple_net_h
#define JetStream_simple_net_h

#include <boost/asio.hpp>
#include "jetstream_types.pb.h"

/**
* A simple synchronous implementation of our protobuf-based network protocol.
* Intended use is for local testing, so there's no error handling.
*/
namespace jetstream {


class SimpleNet {
 boost::asio::ip::tcp::socket &sock;

 public:
   SimpleNet (boost::asio::ip::tcp::socket &s) : sock (s) {}
 
   boost::shared_ptr<DataplaneMessage> get_data_msg ();
   boost::shared_ptr<ControlMessage> get_ctrl_msg ();
  
  /** Try to send. On error, close socket and return error code. */
   boost::system::error_code send_msg (google::protobuf::MessageLite &m);
  
   // This should be a const int, but that interferes with using boost::arrays
   #define HEADER_LEN sizeof(u_int32_t)
   // const int HEADER_LEN = sizeof(u_int32_t);
  
  bool is_connected() { return sock.is_open(); }
  
  void close() {
    sock.close();
  }
};

}


#endif /* Jetstream_simple_net_h */
