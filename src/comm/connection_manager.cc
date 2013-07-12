#include "connection.h"

#include <glog/logging.h>

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
    VLOG(2) << "creating connection to " << domain << endl;
  
    // Domain supplied was a valid IP address
    tcp::resolver::iterator resolved
      = tcp::resolver::iterator::create(tcp::endpoint(addr, port), 
					domain, portstr);
    create_connection (resolved, cb);
  }
  else {
    VLOG(2) << "starting resolve for domain " << domain << endl;

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
  VLOG(2) << "resolved domain" << endl;
  if (!error)
    create_connection(resolved, cb);
  else {
    boost::shared_ptr<ClientConnection> empty;
    cb(empty, error);
  }
}



void
ConnectionManager::create_connection (tcp::resolver::iterator resolved,
				      cb_clntconn_t cb)
{
  if (resolved == tcp::resolver::iterator()) {
    boost::shared_ptr<ClientConnection> empty;
    iosrv->post(bind(cb, empty, asio::error::host_unreachable));
    return;
  }

  tcp::endpoint remote = *resolved++;

  boost::system::error_code error;
  boost::shared_ptr<ClientConnection> c 
    (new ClientConnection (iosrv, remote, error));

  if (!error)
    c->connect(connTimeout,
	       bind(&ConnectionManager::create_connection_cb,
		    this, resolved, c, cb, _1));
  else {
    iosrv->post(bind(&ConnectionManager::create_connection,
		     this, resolved, cb));
  }
}


void
ConnectionManager::create_connection_cb (tcp::resolver::iterator resolved,
					 boost::shared_ptr<ClientConnection> conn,
					 cb_clntconn_t cb,
					 const boost::system::error_code &error)
{
  if (!error)
    cb(conn, error);
  else
    create_connection(resolved, cb);
}
