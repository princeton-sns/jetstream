#ifndef _connected_socket_H_
#define _connected_socket_H_

#include <map>
#include <boost/asio.hpp>
#include <boost/thread.hpp>
#include <boost/shared_ptr.hpp>

#include "js_utils.h"
#include "jetstream_types.pb.h"


#include <glog/logging.h>

namespace jetstream {

typedef google::protobuf::Message ProtobufMessage; //can be either Message or MessageLite

/**
 * Wrapper around C-style char* buf that Protobuf's serialize into / out of,
 * as they cannot take STL vectors or strings.
 */
class SerializedMessageIn {
 public:
  // Parse into serialized protobuf message.
  // Message buf does NOT include leading length
  u_int8_t *msg;
  u_int32_t len;
  SerializedMessageIn (u_int32_t msglen) 
    : msg ((msglen > 0) ? new u_int8_t[msglen] : NULL), len (msglen) {}
  ~SerializedMessageIn () { if (msg) { delete[] msg; } }
  private:
    void operator= (const SerializedMessageIn &) 
      { LOG(FATAL) << "cannot copy a SerializedMessageIn"; }
    SerializedMessageIn (const SerializedMessageIn &) 
      { LOG(FATAL) << "cannot copy a SerializedMessageIn"; }
};


typedef boost::function<void (jetstream::SerializedMessageIn &msg,
			      const boost::system::error_code &) > cb_raw_msg_t;
  
typedef boost::function<void ()> close_cb_t;

/**
 * A ConnectedSocket represents the underlying socket for a connection,
 * and is created after the socket is in its connected state (after connect()
 * returns for clients or accept() returns for servers).
 *
 * Connected sockets primarily serve to read and write data (send and receive
 * messages) in an asynchronous fashion.
 *
 * Sending and receiving maintain separate boost strands, to make sure that
 * only one send- or receive-related function is executing at any one time,
 * even in multi-threaded applications.
 */
class ConnectedSocket : public boost::enable_shared_from_this<ConnectedSocket> {

friend class ClientConnection;
 private:
  boost::shared_ptr<boost::asio::io_service> iosrv;
  boost::shared_ptr<boost::asio::ip::tcp::socket> sock;

  boost::asio::strand sendStrand;
  boost::asio::strand recvStrand;
  cb_raw_msg_t recvcb;
  close_cb_t closing_cb;
  bool isClosing;
  volatile int sendCount; //total number of send operations over lifetime.
        //TODO should be atomic?

  /********* SENDING *********/

  class SerializedMessageOut {
   public:
    static const std::size_t hdrlen = sizeof(u_int32_t);
    u_int8_t *msg;    // Format:  uint32 msg len || serialized protobuf msg
    u_int32_t nbytes; // nbytes = msg_len + serialized protobuf msg
    
    SerializedMessageOut (const ProtobufMessage &m,
			  boost::system::error_code &error);
    ~SerializedMessageOut () { delete[] msg; }
    
   private:
    void operator= (const SerializedMessageOut &) 
      { LOG(FATAL) << "cannot copy a SerializedMessageOut"; }
    SerializedMessageOut (const SerializedMessageOut &) 
      { LOG(FATAL) << "cannot copy a SerializedMessageOut"; }
  };

  // Only one outstanding async_write at a time
  bool sending;

  // Queue of scatter/gather IO pointers, each comprising a single 
  // serialized ProtoBuf message
  /*  As per boost documentation:  The program must ensure that the stream 
  performs no other write operations (such as async_write, the stream's 
  async_write_some function, or any other composed operations that perform writes)
   until this operation [async_write] completes.
  
 From  http://www.boost.org/doc/libs/1_45_0/doc/html/boost_asio/reference/async_write/overload1.html
  */
  std::deque<boost::shared_ptr<SerializedMessageOut> > sendQueue;

  // Serialize access to following functions via same strand
  void perform_send (boost::shared_ptr<SerializedMessageOut> msg);
  void perform_queued_send ();
  void sent (boost::shared_ptr<SerializedMessageOut> msg,
	     const boost::system::error_code &error,
	     size_t bytes_transferred);


  /********* RECEIVING *********/

  // Only one outstanding async_read at a time
  bool receiving;

  // Serialize access to following functions via same strand
  void perform_recv ();
  void received_header (boost::shared_ptr< u_int32_t > hdrbuf,
			const boost::system::error_code &error,
			size_t bytes_transferred);
  void received_body (boost::shared_ptr<SerializedMessageIn> recvMsg,
		      const boost::system::error_code &error,
		      size_t bytes_transferred);

  /********* OTHER SOCKET OPS  *********/

  // Close socket and return error to receive callback if set
  void fail (const boost::system::error_code &error);
  void close_now ();
  void close_on_strand (close_cb_t cb);


 public:
  ConnectedSocket (boost::shared_ptr<boost::asio::io_service> srv,
		   boost::shared_ptr<boost::asio::ip::tcp::socket> s)
    : iosrv (srv), sock (s), sendStrand (*iosrv), recvStrand(*iosrv), 
    isClosing(false),sendCount(0),sending (false), receiving (false) {
    VLOG(1) << "creating connected socket; s " << (s ? "is" : "is not")<< " defined";
  }

  void close(close_cb_t cb) {
    sendStrand.dispatch(bind(&ConnectedSocket::close_on_strand, shared_from_this(), cb));
  }


  // Underlying use of async writes are NOT thread safe; we wrap them here
  void send_msg (const ProtobufMessage &msg,
		 boost::system::error_code &error);

  // Clients of this class use this method to register the receive callback
  // Underlying use of async reads are thread safe
  void recv_msg (cb_raw_msg_t receivecb);
  
  boost::shared_ptr<boost::asio::io_service> get_iosrv () { return iosrv; }

  // FIXME sock->remote/local can through exceptions.  Can with error code instead?
  //TODO: This is throwing during some of our integration tests, since we often
  //just terminate the jsnoded daemon (no way to call Node::stop() gracefully)
  boost::asio::ip::tcp::endpoint get_remote_endpoint () const 
  { return sock->remote_endpoint(); }
  boost::asio::ip::tcp::endpoint get_local_endpoint () const 
  { return sock->local_endpoint(); }

  /**
   * Return socket four tuple in string:  
   * local addr | local port | remote addr | remote port 
   */
  std::string get_fourtuple () const;

  int send_count() { return sendCount; }

};

}

#endif /* _connected_socket_H_ */


