#ifndef _connection_H_
#define _connection_H_

#include <boost/asio.hpp>
#include <boost/thread.hpp>

#include "js_utils.h"
#include "jetstream_types.pb.h"
#include "connected_socket.h"

namespace jetstream {

class ClientConnection;
typedef boost::function<void (boost::shared_ptr<ClientConnection>,
			      const boost::system::error_code &) > cb_clntconn_t;


#if 0
class ServerConnection {
 private:

 public:
  ServerConnection (boost::shared_ptr<boost::asio::io_service> srv,
		    const boost::asio::ip::tcp::endpoint &local,
		    boost::system::error_code &error);

  const boost::asio::ip::tcp::endpoint & get_local_endpoint () const;
};
#endif


class ClientConnection {
 protected:
  bool connected;
  boost::shared_ptr<boost::asio::io_service> iosrv;
  boost::shared_ptr<boost::asio::ip::tcp::socket> sock;
  boost::asio::ip::tcp::endpoint dest;
  boost::asio::deadline_timer timer;

  boost::shared_ptr<ConnectedSocket> conn_sock;

  void connect_cb (cb_err_t cb, const boost::system::error_code &error);
  void timeout_cb (cb_err_t cb, const boost::system::error_code &error);

  cb_protomsg_t recv_cb;

 public:
  ClientConnection (boost::shared_ptr<boost::asio::io_service> srv,
		    const boost::asio::ip::tcp::endpoint &remote,
		    boost::system::error_code &error);
  ~ClientConnection () { close(); }

  void connect (msec_t timeout, cb_err_t cb);
  bool is_connected () const { return connected; }
  const boost::asio::ip::tcp::endpoint & get_remote_endpoint () const 
  { return dest; }

  void close ();

  // Underlying use of async writes are thread safe
  void send_msg (const google::protobuf::Message &msg, 
		 boost::system::error_code &error);
  
  void recv_msg (cb_protomsg_t cb, boost::system::error_code &error);
};



class ConnectionManager {
 private:
  boost::shared_ptr<boost::asio::io_service> iosrv;
  boost::asio::ip::tcp::resolver resolv;
  msec_t conn_timeout; 

  void domain_resolved (cb_clntconn_t cb,
			const boost::system::error_code &error,
			boost::asio::ip::tcp::resolver::iterator dests);

  void create_connection_cb (boost::asio::ip::tcp::resolver::iterator,
			     boost::shared_ptr<ClientConnection> conn,
			     cb_clntconn_t cb,
			     const boost::system::error_code &error);
  
 public:
  ConnectionManager (boost::shared_ptr<boost::asio::io_service> srv,
		     msec_t conn_tmo = 10000)
    : iosrv (srv), resolv (*iosrv), conn_timeout (conn_tmo) {}
  
  void create_connection (const std::string &domain, port_t port,
			  cb_clntconn_t cb);
  void create_connection (boost::asio::ip::tcp::resolver::iterator dests,
			  cb_clntconn_t cb);
};

}

#endif /* _connection_H_ */


