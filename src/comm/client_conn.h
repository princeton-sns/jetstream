#ifndef _client_conn_H_
#define _client_conn_H_

#include <iostream>
#include <map>
#include "js_utils.h"

namespace jetstream {

class ClientConnection;
typedef boost::function<void (boost::shared_ptr<ClientConnection>,
			      const boost::system::error_code &) > bfunc_clntconn;

class ClientConnection {
 private:
  bool connected;
  boost::shared_ptr<boost::asio::io_service> iosrv;
  boost::asio::ip::tcp::endpoint dest;
  boost::asio::ip::tcp::socket sock;
  boost::asio::deadline_timer timer;

  void connect_cb (bfunc_err cb, const boost::system::error_code &error);
  void timeout_cb (bfunc_err cb, const boost::system::error_code &error);

 public:
  ClientConnection (boost::shared_ptr<boost::asio::io_service> srv,
		    const boost::asio::ip::tcp::endpoint &d,
		    boost::system::error_code &error);

  void connect (msec_t timeout, bfunc_err cb);
  void close (boost::system::error_code &error);

  bool is_connected () const { return connected; }
  const boost::asio::ip::tcp::endpoint & get_endpoint () const { return dest; }
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


