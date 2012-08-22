#ifndef _connection_H_
#define _connection_H_

#include <boost/asio.hpp>
#include <boost/thread.hpp>

#include "js_utils.h"
#include "future_js.pb.h"
#include "connected_socket.h"

namespace jetstream {

class ClientConnection;

typedef boost::function<void (boost::shared_ptr<ClientConnection>,
			      const boost::system::error_code &) > cb_clntconn_t;

typedef boost::function<void (boost::shared_ptr<ConnectedSocket>,
			      const boost::system::error_code &) > cb_connsock_t;


class ServerConnection {
 private:
  bool accepting;
  boost::shared_ptr<boost::asio::io_service> iosrv;
  boost::asio::ip::tcp::acceptor srv_acceptor;
  boost::asio::ip::tcp::endpoint local;
  boost::asio::strand astrand;

  std::map<boost::asio::ip::tcp::endpoint, 
    boost::shared_ptr<ConnectedSocket> > clients;

  cb_connsock_t accept_cb;

  // Wrap do_accept and accepted in same strand
  void do_accept ();
  void accepted (boost::shared_ptr<boost::asio::ip::tcp::socket> new_sock,
		 const boost::system::error_code &error);

 public:
  ServerConnection (boost::shared_ptr<boost::asio::io_service> srv,
		    const boost::asio::ip::tcp::endpoint &local_end,
		    boost::system::error_code &error);
  ~ServerConnection ();

  const boost::asio::ip::tcp::endpoint & get_local_endpoint () const
  { return local; }

  void accept (cb_connsock_t cb, boost::system::error_code &error);
  bool is_accepting () const { return accepting; }
  void close ();
};


class ClientConnection {
 protected:
  bool connected;
  boost::shared_ptr<boost::asio::io_service> iosrv;
  boost::shared_ptr<boost::asio::ip::tcp::socket> sock;
  boost::asio::ip::tcp::endpoint remote;
  boost::asio::deadline_timer timer;

  boost::shared_ptr<ConnectedSocket> conn_sock;

  void connect_cb (cb_err_t cb, const boost::system::error_code &error);
  void timeout_cb (cb_err_t cb, const boost::system::error_code &error);

 public:
  ClientConnection (boost::shared_ptr<boost::asio::io_service> srv,
		    const boost::asio::ip::tcp::endpoint &remote_end,
		    boost::system::error_code &error);
  ~ClientConnection () { close(); }

  const boost::asio::ip::tcp::endpoint & get_remote_endpoint () const 
  { return remote; }

  void connect (msec_t timeout, cb_err_t cb);
  bool is_connected () const { return connected; }
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
			boost::asio::ip::tcp::resolver::iterator resolved);

  void create_connection_cb (boost::asio::ip::tcp::resolver::iterator resolved,
			     boost::shared_ptr<ClientConnection> conn,
			     cb_clntconn_t cb,
			     const boost::system::error_code &error);
  
 public:
  ConnectionManager (boost::shared_ptr<boost::asio::io_service> srv,
		     msec_t conn_tmo = 10000)
    : iosrv (srv), resolv (*iosrv), conn_timeout (conn_tmo) {}
  
  void create_connection (const std::string &domain, port_t port,
			  cb_clntconn_t cb);
  void create_connection (boost::asio::ip::tcp::resolver::iterator resolved,
			  cb_clntconn_t cb);
};

}

#endif /* _connection_H_ */


