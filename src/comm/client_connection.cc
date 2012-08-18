#include "connection.h"

using namespace std;
using namespace boost;
using namespace boost::asio::ip;
using namespace jetstream;


ClientConnection::ClientConnection (shared_ptr<asio::io_service> srv,
				    const tcp::endpoint &remote,
				    system::error_code &error)
  : connected (false), iosrv (srv), sock (new tcp::socket(*iosrv)),
    dest (remote), timer (*iosrv)
{
  if (dest.address().is_v4())
    sock->open(tcp::v4(), error);
  else if (dest.address().is_v6())
    sock->open(tcp::v6(), error);
  else
    error = asio::error::address_family_not_supported;
}


void
ClientConnection::connect (msec_t timeout, cb_err_t cb)
{
  if (!sock->is_open()) {
    iosrv->post(bind(cb, asio::error::address_family_not_supported));
    return;
  }

  // Set a deadline for the connect operation.
  timer.expires_from_now(posix_time::milliseconds(timeout));

  // Start the asynchronous connect operation.
  sock->async_connect(dest,
		     bind(&ClientConnection::connect_cb, this, cb, _1));

  timer.async_wait(bind(&ClientConnection::timeout_cb, this, cb, _1));
}


void 
ClientConnection::connect_cb (cb_err_t cb, 
			      const system::error_code &error)
{
  if (error == asio::error::operation_aborted)
    return;

  // Unregister timeout
  system::error_code e;
  timer.cancel(e);

  if (error)
    close();
  else
    connected = true;

  shared_ptr<ConnectedSocket> cs (new ConnectedSocket(iosrv, sock, dest));
  conn_sock = cs;

  cb(error);
}


void
ClientConnection::timeout_cb (cb_err_t cb,
			      const system::error_code &error)
{
  if (error == asio::error::operation_aborted)
    return;

  // close() will cancel async_connect
  close();
  cb(asio::error::timed_out);
}
  

void 
ClientConnection::close ()
{
  connected = false;

  if (conn_sock)
    conn_sock->close();
  else if (sock->is_open()) {
    system::error_code error;
    sock->cancel(error);
    sock->shutdown(tcp::socket::shutdown_both, error);
    sock->close(error);
  }
}


void 
ClientConnection::send_msg (const google::protobuf::Message &msg, 
			    boost::system::error_code &error)
{
  if (!connected || !conn_sock) {
    error = asio::error::not_connected;
    return;
  }

  conn_sock->send_msg(msg, error);
}




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



