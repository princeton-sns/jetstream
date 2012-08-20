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
    tcp::resolver::iterator resolved
      = tcp::resolver::iterator::create(tcp::endpoint(addr, port), 
					domain, portstr);
    create_connection (resolved, cb);
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
				    tcp::resolver::iterator resolved)
{
  if (!error)
    create_connection(resolved, cb);
  else {
    shared_ptr<ClientConnection> empty;
    cb(empty, error);
  }
}



void
ConnectionManager::create_connection (tcp::resolver::iterator resolved,
				      cb_clntconn_t cb)
{
  if (resolved == tcp::resolver::iterator()) {
    shared_ptr<ClientConnection> empty;
    iosrv->post(bind(cb, empty, asio::error::host_unreachable));
    return;
  }

  tcp::endpoint remote = *resolved++;

  boost::system::error_code error;
  shared_ptr<ClientConnection> c 
    (new ClientConnection (iosrv, remote, error));

  if (!error)
    c->connect (conn_timeout,
		bind(&ConnectionManager::create_connection_cb,
		     this, resolved, c, cb, _1));
  else
    iosrv->post(bind(&ConnectionManager::create_connection,
		     this, resolved, cb));
}


void
ConnectionManager::create_connection_cb (tcp::resolver::iterator resolved,
					 shared_ptr<ClientConnection> conn,
					 cb_clntconn_t cb,
					 const boost::system::error_code &error)
{
  if (!error)
    cb(conn, error);
  else
    create_connection(resolved, cb);
}
