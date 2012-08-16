#include "client_conn.h"

using namespace std;
using namespace boost;
using namespace boost::asio::ip;
using namespace jetstream;


#if 0
boost::shared_ptr<ClientConnection> get_connection (const boost::asio::ip::tcp::endpoint &dest);

shared_ptr<ClientConnection>
ClientConnectionManager::get_connection (const tcp::endpoint &dest)
{
  map<tcp::endpoint, shared_ptr<ClientConnection> >::iterator iter = conns.find (dest);
  if (iter == conns.end())
    return shared_ptr<ClientConnection> ();
  else
    return iter->second;
}
#endif


void
ClientConnectionManager::create_connection (const string &domain, port_t port,
					    bfunc_clntconn cb)
{
  string portstr = lexical_cast<string> (port);

  system::error_code error;
  address addr = address::from_string(domain, error);

  if (!error) {
    // Domain supplied was a valid IP address
    tcp::resolver::iterator dests 
      = tcp::resolver::iterator::create(tcp::endpoint(addr, port), domain, portstr);
    create_connection (dests, cb);
  }
  else {
    // Need to perform DNS resolution
    tcp::resolver::query q(domain, portstr);
    resolv.async_resolve(q, 
			 bind(&ClientConnectionManager::domain_resolved, 
			      this, cb, _1, _2));
  }
}


void
ClientConnectionManager::domain_resolved (bfunc_clntconn cb,
					  const system::error_code &error,
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
ClientConnectionManager::create_connection (tcp::resolver::iterator dests,
					    bfunc_clntconn cb)
{
  if (dests == tcp::resolver::iterator()) {
    shared_ptr<ClientConnection> empty;
    iosrv->post(bind(cb, empty, asio::error::host_unreachable));
    return;
  }

  tcp::endpoint dest = *dests++;

  system::error_code error;
  shared_ptr<ClientConnection> c 
    (new ClientConnection (iosrv, dest, error));

  if (!error)
    c->connect (conn_timeout,
		bind(&ClientConnectionManager::create_connection_cb,
		     this, dests, c, cb, _1));
  else
    iosrv->post(bind(&ClientConnectionManager::create_connection,
		     this, dests, cb));
}


void
ClientConnectionManager::create_connection_cb (tcp::resolver::iterator dests,
					       shared_ptr<ClientConnection> conn,
					       bfunc_clntconn cb,
					       const system::error_code &error)
{
  if (!error)
    cb(conn, error);
  else
    create_connection(dests, cb);
}


ClientConnection::ClientConnection (shared_ptr<asio::io_service> srv,
				    const tcp::endpoint &d,
				    system::error_code &error)
  : connected (false), iosrv (srv), 
    dest (d), sock (*iosrv), timer (*iosrv)
{
  if (dest.address().is_v4())
    sock.open(tcp::v4(), error);
  else if (dest.address().is_v6())
    sock.open(tcp::v6(), error);
  else
    error = asio::error::address_family_not_supported;
}



void
ClientConnection::connect (msec_t timeout, bfunc_err cb)
{
  if (!sock.is_open()) {
    iosrv->post(bind(cb, asio::error::address_family_not_supported));
    return;
  }

  // Set a deadline for the connect operation.
  timer.expires_from_now(posix_time::milliseconds(timeout));

  // Start the asynchronous connect operation.
  sock.async_connect(dest,
		     bind(&ClientConnection::connect_cb, this, cb, _1));

  timer.async_wait(bind(&ClientConnection::timeout_cb, this, cb, _1));
}


void 
ClientConnection::connect_cb (bfunc_err cb, 
			      const system::error_code &error)
{
  // Unregister timeout
  system::error_code e;
  timer.cancel(e);

  if (error)
    close(e);
  else
    connected = true;

  cb(error);
}


void
ClientConnection::timeout_cb(bfunc_err cb,
			     const system::error_code &error)
{
  if (error == asio::error::operation_aborted)
    return;

  system::error_code e;
  close(e);

  cb(asio::error::timed_out);
}
  

void 
ClientConnection::close (system::error_code &error)
{
  connected = false;

  if (sock.is_open())
    sock.shutdown(tcp::socket::shutdown_both, error);
  
  sock.close (error);
}


