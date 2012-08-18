#ifndef _client_conn_H_
#define _client_conn_H_

#include <iostream>
#include <map>
#include <boost/asio.hpp>
#include <boost/thread.hpp>
#include <boost/shared_array.hpp>
#include "js_utils.h"
#include "jetstream_types.pb.h"

namespace jetstream {

class ClientConnection;
typedef boost::function<void (boost::shared_ptr<ClientConnection>,
			      const boost::system::error_code &) > bfunc_clntconn;


class ClientConnection {
 private:
  bool connected;
  boost::shared_ptr<boost::asio::io_service> iosrv;
  boost::asio::strand astrand;
  boost::asio::ip::tcp::endpoint dest;
  boost::asio::ip::tcp::socket sock;
  boost::asio::deadline_timer timer;

  class SerializedMessage;

  // Queue of scatter/gather IO pointers, each comprising a single 
  // serialized ProtoBuf message
  std::deque<boost::shared_ptr<SerializedMessage> > write_queue;

  // Only one outstanding async_write at a time
  bool writing;

  void connect_cb (bfunc_err cb, const boost::system::error_code &error);
  void timeout_cb (bfunc_err cb, const boost::system::error_code &error);

  // Serialize access to following functions via strand
  void perform_write (boost::shared_ptr<SerializedMessage> msg);
  void perform_queued_write ();
  void wrote (boost::shared_ptr<SerializedMessage> msg,
	      const boost::system::error_code &error,
	      size_t bytes_transferred);


  class SerializedMessage {
   public:
    u_int8_t *msg;    // Format:  uint32 mmsg len || serialized protobuf msg
    u_int32_t nbytes; // Size of allocated msg = len || serialization
    SerializedMessage (const google::protobuf::Message &m,
		       boost::system::error_code &error);
    ~SerializedMessage () { delete[] msg; }
  };

 public:
  ClientConnection (boost::shared_ptr<boost::asio::io_service> srv,
		    const boost::asio::ip::tcp::endpoint &d,
		    boost::system::error_code &error);

  void connect (msec_t timeout, bfunc_err cb);
  void close (boost::system::error_code &error);

  bool is_connected () const { return connected; }
  const boost::asio::ip::tcp::endpoint & get_endpoint () const { return dest; }

  // Underlying use of async writes are thread safe
  void write_msg (const google::protobuf::Message &msg, 
		  boost::system::error_code &error);

  void read_msg (google::protobuf::Message &msg,
		 boost::system::error_code &error);
};


class ClientConnectionManager {
 private:
  boost::shared_ptr<boost::asio::io_service> iosrv;
  boost::asio::ip::tcp::resolver resolv;
  msec_t conn_timeout; 

  void domain_resolved (bfunc_clntconn cb,
			const boost::system::error_code &error,
			boost::asio::ip::tcp::resolver::iterator dests);

  void create_connection_cb (boost::asio::ip::tcp::resolver::iterator,
			     boost::shared_ptr<ClientConnection> conn,
			     bfunc_clntconn cb,
			     const boost::system::error_code &error);
  
 public:
  ClientConnectionManager (boost::shared_ptr<boost::asio::io_service> srv,
			   msec_t conn_tmo = 10000)
    : iosrv (srv), resolv (*iosrv), conn_timeout (conn_tmo) {}
  
  void create_connection (const std::string &domain, port_t port,
			  bfunc_clntconn cb);
  void create_connection (boost::asio::ip::tcp::resolver::iterator dests,
			  bfunc_clntconn cb);
};

}

#endif /* _client_conn_H_ */


