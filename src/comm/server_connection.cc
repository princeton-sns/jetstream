#include "connection.h"

using namespace std;
using namespace boost;
using namespace boost::asio::ip;
using namespace jetstream;


ServerConnection::ServerConnection (shared_ptr<boost::asio::io_service> srv,
				    const tcp::endpoint &localEndpoint,
				    boost::system::error_code &error)
  : accepting (false), iosrv (srv), srvAcceptor (*iosrv), 
    local (localEndpoint), astrand (*iosrv)
{
  if (local.address().is_v4())
    srvAcceptor.open(tcp::v4(), error);
  else if (local.address().is_v6())
    srvAcceptor.open(tcp::v6(), error);
  else
    error = asio::error::address_family_not_supported;

  if (error) return;
  srvAcceptor.set_option(tcp::acceptor::reuse_address(true), error);
  if (error) return;
  srvAcceptor.bind(local, error);

  // User might have passed port 0, so make sure endpoint has real port info
  local = srvAcceptor.local_endpoint();  

  if (error) return;
  srvAcceptor.listen(asio::socket_base::max_connections, error);
}


ServerConnection::~ServerConnection ()
{
  if (acceptcb) {
    shared_ptr<ConnectedSocket> empty;
    acceptcb(empty, asio::error::eof);
  }
  close();
}


void
ServerConnection::accept (cb_connsock_t cb, boost::system::error_code &error)
{
  if (!srvAcceptor.is_open()) {
    error = asio::error::address_family_not_supported;
    return;
  }
  else if (accepting) {
    error = asio::error::already_started;
    return;
  }

  acceptcb = cb;

  astrand.post(bind(&ServerConnection::do_accept, this));
}


void
ServerConnection::do_accept ()
{
  if (!srvAcceptor.is_open() || accepting) {
    return;
  }

  accepting = true;

  shared_ptr<tcp::socket> newSock (new tcp::socket(*iosrv));
  srvAcceptor.async_accept(*newSock,
			    astrand.wrap(boost::bind(&ServerConnection::accepted, 
						     this, newSock, _1)));
      
}



void
ServerConnection::accepted (shared_ptr<tcp::socket> newSock,
			    const boost::system::error_code &error)
{
  accepting = false;

  if (!acceptcb) {
    close();
    return;
  }

  if (error) {
    // XXX Temp or permanent error?
    close();
    shared_ptr<ConnectedSocket> empty;
    acceptcb (empty, error);
    return;
  }
  
  // Schedule next accept
  astrand.post(bind(&ServerConnection::do_accept, this));

  // Process this new connection
  boost::system::error_code ec;
  tcp::endpoint remote = newSock->remote_endpoint(ec);

  if (ec) {
    // XXX Temp or permanent erorr
    shared_ptr<ConnectedSocket> empty;
    acceptcb (empty, ec);
    return;
  }
  else if (clients.find(remote) != clients.end()) {
    // Map shouldn't already have an entry.  Error.
    // return error through acceptcb()?
    return;
  }

  shared_ptr<ConnectedSocket> connSock (new ConnectedSocket(iosrv, newSock));
  clients[remote] = connSock;
  boost::system::error_code success;
  acceptcb(connSock, success);
}


void
ServerConnection::close ()
{
  accepting = false;
  acceptcb = NULL;

  if (srvAcceptor.is_open()) {
    boost::system::error_code error;
    srvAcceptor.cancel(error);
    srvAcceptor.close(error);
  }
}
