#ifndef _client_conn_H_
#define _client_conn_H_

#include <iostream>
#include <map>
#include "js_utils.h"

namespace jetstream {

class ClientConnection {
 private:
  boost::shared_ptr<boost::asio::io_service> iosrv;
  boost::asio::strand astrand;
  boost::asio::ip::tcp::endpoint dest;
  boost::asio::ip::tcp::socket sock;
  boost::asio::deadline_timer timer;

  void connect_cb (bfunc_void_err cb, const boost::system::error_code &error);
  void timeout_cb (bfunc_void_err cb);

 public:
  ClientConnection (boost::shared_ptr<boost::asio::io_service> srv,
		    const boost::asio::ip::tcp::endpoint &d,
		    boost::system::error_code &error);

  void connect (msec_t timeout, bfunc_void_err cb);

  void close (boost::system::error_code &error);
};


class ClientConnectionPool {
 private:
  boost::shared_ptr<boost::asio::io_service> iosrv;
  std::map<boost::asio::ip::tcp::endpoint,
    boost::shared_ptr<ClientConnection> > pool;

  void domain_resolved (bfunc_void_endpoint cb,
			const boost::system::error_code &error,
			boost::asio::ip::tcp::resolver::iterator dests);

  //  void create_connection_cb (boost::asio::ip::tcp::endpoint dest,
  //			     bfunc_void_endpoint cb,
  //			     const boost::system::error_code &error);

  void create_connection_cb (boost::asio::ip::tcp::endpoint dest,
			     boost::asio::ip::tcp::resolver::iterator,
			     bfunc_void_endpoint cb,
			     const boost::system::error_code &error);
  
 public:
  ClientConnectionPool (boost::shared_ptr<boost::asio::io_service> srv) 
    : iosrv (srv) {}

  void create_connection (const std::string &domain, port_t port,
			  bfunc_void_endpoint cb);
  void create_connection (boost::asio::ip::tcp::resolver::iterator dests,
			  bfunc_void_endpoint cb);
  //  void create_connection (const boost::asio::ip::tcp::endpoint &dest,
  //  			  bfunc_void_endpoint cb);

};

}

#endif /* _client_conn_H_ */


