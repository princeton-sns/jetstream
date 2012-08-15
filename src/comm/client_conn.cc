#include "client_conn.h"

using namespace boost::asio::ip;
using namespace jetstream;

void
ClientConnectionPool::create_connection (const std::string &domain, port_t port,
					 bfunc_void_endpoint cb)
{
  std::string portstr = boost::lexical_cast<std::string> (port);

  boost::system::error_code error;
  address addr = address::from_string(domain, error);

  if (!error) {
    // Domain supplied was a valid IP address
    tcp::resolver::iterator dests 
      = tcp::resolver::iterator::create(tcp::endpoint(addr, port), domain, portstr);
    create_connection (dests, cb);
  }
  else {
    // Need to perform DNS resolution
    tcp::resolver resolv(*iosrv);
    tcp::resolver::query q(domain, portstr);

    resolv.async_resolve(q, 
			 boost::bind(&ClientConnectionPool::domain_resolved, 
				     this, cb, _1, _2));
  }
}


void
ClientConnectionPool::domain_resolved (bfunc_void_endpoint cb,
				       const boost::system::error_code &error,
				       tcp::resolver::iterator dests)
{
  if (error) {
    cb (tcp::endpoint(), error);
    return;
  }
  
  create_connection(dests, cb);
}



void
ClientConnectionPool::create_connection (tcp::resolver::iterator dests,
					 bfunc_void_endpoint cb)
{
  if (dests == tcp::resolver::iterator()) {
    iosrv->post(boost::bind(cb, tcp::endpoint(), 
			    boost::asio::error::host_unreachable));
    return;
  }

  tcp::endpoint dest = *dests++;

  boost::system::error_code error;
  boost::shared_ptr<ClientConnection> c 
    (new ClientConnection (iosrv, dest, error));

  if (error)
    iosrv->post(boost::bind(&ClientConnectionPool::create_connection,
			    this, dests, cb));
  else {
    pool[dest] = c;
    c->connect (10000, 
		boost::bind(&ClientConnectionPool::create_connection_cb,
			    this, dest, dests, cb, _1));
  }
}


void
ClientConnectionPool::create_connection_cb (tcp::endpoint dest,
					    tcp::resolver::iterator dests,
					    bfunc_void_endpoint cb,
					    const boost::system::error_code &error)
{
  if (!error)
    cb (dest, boost::asio::error::not_found);
  else {
    pool.erase(dest);
    create_connection (dests, cb);
  }
}

#if 0
void
ClientConnectionPool::create_connection (const tcp::endpoint &dest,
					 bfunc_void_endpoint cb)
{
  boost::system::error_code error;
  boost::shared_ptr<ClientConnection> c 
    (new ClientConnection (iosrv, dest, error));

  if (error)
    iosrv->post(boost::bind(cb, tcp::endpoint(), error));
  else {
    // Keep at least one reference to the ClientConnection around
    pool[dest] = c;
    c->connect (10000, 
		boost::bind (&ClientConnectionPool::create_connection_cb,
			     this, dest, cb, _1));
  }
}


void
ClientConnectionPool::create_connection_cb (tcp::endpoint dest,
					    bfunc_void_endpoint cb,
					    const boost::system::error_code &error)
{
  if (error)
    // Erase map reference to connection
    pool.erase (dest);

  cb (error);
}
#endif


ClientConnection::ClientConnection (boost::shared_ptr<boost::asio::io_service> srv,
				    const tcp::endpoint &d,
				    boost::system::error_code &error)
  : iosrv (srv), astrand (*iosrv), dest (d), sock (*iosrv), timer (*iosrv)
{
  if (dest.address().is_v4())
    sock.open(tcp::v4(), error);
  else if (dest.address().is_v6())
    sock.open(tcp::v6(), error);
  else
    error = boost::asio::error::address_family_not_supported;
}



void
ClientConnection::connect (msec_t timeout, bfunc_void_err cb)
{
  if (!sock.is_open()) {
    iosrv->post(boost::bind(cb, boost::asio::error::address_family_not_supported));
    return;
  }

  // Set a deadline for the connect operation.
  timer.expires_from_now(boost::posix_time::milliseconds(timeout));

  // Start the asynchronous connect operation.
  sock.async_connect(dest,
		     boost::bind(&ClientConnection::connect_cb, this, cb, _1));

  timer.async_wait(boost::bind(&ClientConnection::timeout_cb, this, cb));
}


void 
ClientConnection::connect_cb (bfunc_void_err cb, 
			      const boost::system::error_code &error)
{
  // Unregister timeout
  boost::system::error_code e;
  timer.cancel(e);

  if (error) {
    close(e);
  }

  cb (error);
}


void
ClientConnection::timeout_cb (bfunc_void_err cb)
{
  boost::system::error_code error;
  close(error);

  cb (boost::asio::error::timed_out);
}
  

void 
ClientConnection::close (boost::system::error_code &error)
{
  if (sock.is_open()) {
    sock.shutdown(tcp::socket::shutdown_both, error);

    if (error) {
      std::cerr << "ClientConnection::close: " << error << std::endl;
      // return?
    }
  }
  
  sock.close (error);
  
  if (error) {
    std::cerr << "ClientConnection::do_close: " << error << std::endl;
  }
}
