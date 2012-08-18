#include "connection.h"

using namespace std;
using namespace boost;
using namespace boost::asio::ip;
using namespace jetstream;

void
ConnectionManager::create_connection (const string &domain, port_t port,
				      cb_clntconn_t cb)
{
  string portstr = lexical_cast<string> (port);

  boost::system::error_code error;
  address addr = address::from_string(domain, error);

  if (!error) {
    // Domain supplied was a valid IP address
    tcp::resolver::iterator dests 
      = tcp::resolver::iterator::create(tcp::endpoint(addr, port), 
					domain, portstr);
    create_connection (dests, cb);
  }
  else {
    // Need to perform DNS resolution
    tcp::resolver::query q(domain, portstr);
    resolv.async_resolve(q, 
			 bind(&ConnectionManager::domain_resolved, 
			      this, cb, _1, _2));
  }
}


void
ConnectionManager::domain_resolved (cb_clntconn_t cb,
				    const boost::system::error_code &error,
				    tcp::resolver::iterator dests)
{
  if (!error)
    create_connection(dests, cb);
  else {
    shared_ptr<ClientConnection> empty;
    cb(empty, error);
  }
}



void
ConnectionManager::create_connection (tcp::resolver::iterator dests,
				      cb_clntconn_t cb)
{
  if (dests == tcp::resolver::iterator()) {
    shared_ptr<ClientConnection> empty;
    iosrv->post(bind(cb, empty, asio::error::host_unreachable));
    return;
  }

  tcp::endpoint dest = *dests++;

  boost::system::error_code error;
  shared_ptr<ClientConnection> c 
    (new ClientConnection (iosrv, dest, error));

  if (!error)
    c->connect (conn_timeout,
		bind(&ConnectionManager::create_connection_cb,
		     this, dests, c, cb, _1));
  else
    iosrv->post(bind(&ConnectionManager::create_connection,
		     this, dests, cb));
}


void
ConnectionManager::create_connection_cb (tcp::resolver::iterator dests,
					 shared_ptr<ClientConnection> conn,
					 cb_clntconn_t cb,
					 const boost::system::error_code &error)
{
  if (!error)
    cb(conn, error);
  else
    create_connection(dests, cb);
}
