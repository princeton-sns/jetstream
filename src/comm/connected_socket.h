#ifndef _connected_socket_H_
#define _connected_socket_H_

#include <map>
#include <boost/asio.hpp>
#include <boost/thread.hpp>
#include <boost/shared_ptr.hpp>

#include "js_utils.h"
#include "jetstream_types.pb.h"

namespace jetstream {

typedef boost::function<void (google::protobuf::Message &msg,
			      const boost::system::error_code &) > cb_protomsg_t;


class ConnectedSocket : public boost::enable_shared_from_this<ConnectedSocket> {
 private:
  boost::shared_ptr<boost::asio::io_service> iosrv;
  boost::shared_ptr<boost::asio::ip::tcp::socket> sock;

  boost::asio::strand astrand;
  cb_protomsg_t recv_cb;


  /********* SENDING *********/

  class SerializedMessageOut {
   public:
    static const std::size_t hdrlen = sizeof(u_int32_t);
    u_int8_t *msg;    // Format:  uint32 msg len || serialized protobuf msg
    u_int32_t nbytes; // nbytes = msg_len + serialized protobuf msg
    SerializedMessageOut (const google::protobuf::Message &m,
			  boost::system::error_code &error);
    ~SerializedMessageOut () { delete[] msg; }
  };

  // Only one outstanding async_write at a time
  bool sending;

  // Queue of scatter/gather IO pointers, each comprising a single 
  // serialized ProtoBuf message
  std::deque<boost::shared_ptr<SerializedMessageOut> > send_queue;

  // Serialize access to following functions via same strand
  void perform_send (boost::shared_ptr<SerializedMessageOut> msg);
  void perform_queued_send ();
  void sent (boost::shared_ptr<SerializedMessageOut> msg,
	     const boost::system::error_code &error,
	     size_t bytes_transferred);


  /********* RECEIVING *********/

  class SerializedMessageIn {
   public:
    // Parse into serialized protobuf message.
    // Message buf does NOT include leading length
    u_int8_t *msg;
    u_int32_t len;
    SerializedMessageIn (u_int32_t msglen) 
      : msg (new u_int8_t[len]), len (msglen) {}
    ~SerializedMessageIn () { if (msg) delete[] msg; }
  };

  // Only one outstanding async_read at a time
  bool receiving;

  // Serialize access to following functions via same strand
  void perform_recv ();
  void received_header (boost::shared_ptr<std::vector<u_int32_t> > hdrbuf,
			const boost::system::error_code &error,
			size_t bytes_transferred);
  void received_body (boost::shared_ptr<SerializedMessageIn> recv_msg,
		      const boost::system::error_code &error,
		      size_t bytes_transferred);

  /********* OTHER SOCKET OPS  *********/

  // Close socket and return error to receive callback if set
  void fail (const boost::system::error_code &error);

 public:
  ConnectedSocket (boost::shared_ptr<boost::asio::io_service> srv,
		   boost::shared_ptr<boost::asio::ip::tcp::socket> s)
    : iosrv (srv), sock (s), astrand (*iosrv) {}

  void close ();

  // Underlying use of async writes are thread safe
  void send_msg (const google::protobuf::Message &msg, 
		 boost::system::error_code &error);

  // Underlying use of async reads are thread safe
  void recv_msg (cb_protomsg_t recvcb);
};


}

#endif /* _connected_socket_H_ */


