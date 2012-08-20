#include "connection.h"

using namespace std;
using namespace boost;
using namespace boost::asio::ip;
using namespace jetstream;


ServerConnection::ServerConnection (shared_ptr<boost::asio::io_service> srv,
				    const tcp::endpoint &local_end,
				    system::error_code &error)
  : accepting (false), iosrv (srv), srv_acceptor (*iosrv), 
    local (local_end), astrand (*iosrv)
{
  if (local.address().is_v4())
    srv_acceptor.open(tcp::v4(), error);
  else if (local.address().is_v6())
    srv_acceptor.open(tcp::v6(), error);
  else
    error = asio::error::address_family_not_supported;

  if (error) return;
  srv_acceptor.set_option(tcp::acceptor::reuse_address(true), error);
  if (error) return;
  srv_acceptor.bind(local, error);
  if (error) return;
  srv_acceptor.listen(asio::socket_base::max_connections, error);
}


ServerConnection::~ServerConnection ()
{
  if (accept_cb) {
    shared_ptr<ConnectedSocket> empty;
    accept_cb(empty, asio::error::eof);
  }
  close();
}


void
ServerConnection::accept (cb_connsock_t cb, system::error_code &error)
{
  if (!srv_acceptor.is_open()) {
    error = asio::error::address_family_not_supported;
    return;
  }
  else if (accepting) {
    error = asio::error::already_started;
    return;
  }

  accept_cb = cb;

  astrand.post(bind(&ServerConnection::do_accept, this));
}


void
ServerConnection::do_accept ()
{
  if (srv_acceptor.is_open() || accepting) {
    return;
  }

  accepting = true;

  shared_ptr<tcp::socket> new_sock (new tcp::socket(*iosrv));
  srv_acceptor.async_accept(*new_sock,
			    astrand.wrap(boost::bind(&ServerConnection::accepted, 
						     this, new_sock, _1)));
}



void
ServerConnection::accepted (shared_ptr<tcp::socket> new_sock,
			    const system::error_code &error)
{
  accepting = false;

  if (!accept_cb) {
    close();
    return;
  }

  if (error) {
    // XXX Temp or permanent error?
    close();
    shared_ptr<ConnectedSocket> empty;
    accept_cb (empty, error);
    return;
  }
  
  // Schedule next accept
  astrand.post(bind(&ServerConnection::do_accept, this));

  // Process this new connection
  system::error_code ec;
  tcp::endpoint remote = new_sock->remote_endpoint(ec);

  if (ec) {
    // XXX Temp or permanent erorr
    shared_ptr<ConnectedSocket> empty;
    accept_cb (empty, ec);
    return;
  }
  else if (clients.find(remote) != clients.end()) {
    // Map shouldn't already have an entry.  Error.
    // return error through accept_cb()?
    return;
  }

  shared_ptr<ConnectedSocket> conn_sock (new ConnectedSocket(iosrv, new_sock));
  clients[remote] = conn_sock;
  system::error_code success;
  accept_cb(conn_sock, success);
}


void
ServerConnection::close ()
{
  accepting = false;
  accept_cb = NULL;

  if (srv_acceptor.is_open()) {
    system::error_code error;
    srv_acceptor.cancel(error);
    srv_acceptor.close(error);
  }
}
