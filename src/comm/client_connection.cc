#include "connection.h"

#include <glog/logging.h>

using namespace std;
using namespace boost;
using namespace boost::asio::ip;
using namespace jetstream;


ClientConnection::ClientConnection (shared_ptr<asio::io_service> srv,
				    const tcp::endpoint &remote_end,
				    boost::system::error_code &error)
  : connected (false), iosrv (srv), sock (new tcp::socket(*iosrv)),
    remote (remote_end), timer (*iosrv)
{
  if (remote.address().is_v4())
    sock->open(tcp::v4(), error);
  else if (remote.address().is_v6())
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
  VLOG(2) << "client_connection connect starting async connect" <<endl;

  // Set a deadline for the connect operation.
  timer.expires_from_now(posix_time::milliseconds(timeout));

  // Start the asynchronous connect operation.
  sock->async_connect(remote,
		     bind(&ClientConnection::connect_cb, this, cb, _1));

  timer.async_wait(bind(&ClientConnection::timeout_cb, this, cb, _1));
}


void 
ClientConnection::connect_cb (cb_err_t cb, 
			      const boost::system::error_code &error)
{
  VLOG(2) << "client_connection connect_cb" <<endl;

  if (error == asio::error::operation_aborted)
    return;

  // Unregister timeout
  boost::system::error_code ec;
  timer.cancel(ec);

  if (error)
    close();
  else
    connected = true;

  shared_ptr<ConnectedSocket> cs (new ConnectedSocket(iosrv, sock));
  conn_sock = cs;

  cb(error);
}


void
ClientConnection::timeout_cb (cb_err_t cb,
			      const boost::system::error_code &error)
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
    boost::system::error_code error;
    sock->cancel(error);
    sock->shutdown(tcp::socket::shutdown_both, error);
    sock->close(error);
  }
}

void 
ClientConnection::send_msg (const ProtobufMessage &msg,
			    boost::system::error_code &error)
{
  if (!connected || !conn_sock) {
    error = asio::error::not_connected;
    return;
  }

  conn_sock->send_msg(msg, error);
}


//NOTE: this method is an implementation detail and not defined in any header.
void
parse_data(cb_data_protomsg_t cb, jetstream::SerializedMessageIn &msg,
			      const boost::system::error_code &)
{
  boost::system::error_code success;
  DataplaneMessage req;
  req.ParseFromArray(msg.msg, msg.len);
  cb(req, success);
}


void
ClientConnection::recv_data_msg (cb_data_protomsg_t cb,
			    boost::system::error_code &error)
{
  if (!connected || !conn_sock) {
    error = asio::error::not_connected;
    return;
  }
  conn_sock->recv_msg(boost::bind(&parse_data, cb, _1, _2));
}

//NOTE: this method is an implementation detail and not defined in any header.
void
parse_control(cb_control_protomsg_t cb, jetstream::SerializedMessageIn &msg,
			      const boost::system::error_code & e)
{
  boost::system::error_code success;
  ControlMessage req;
  if (msg.len > 0) {
    req.ParseFromArray(msg.msg, msg.len);
    cb(req, success);
  }
  else {
    //TODO: what if we're closing the connection and the rror should be just ignored?
    LOG(INFO)<< "unexpected error in client_connection::parse_control; error code was " << e <<endl;
  }
}


void
ClientConnection::recv_ctrl_msg (cb_control_protomsg_t cb,
			    boost::system::error_code &error)
{
  if (!connected || !conn_sock) {
    error = asio::error::not_connected;
    return;
  }
  conn_sock->recv_msg(boost::bind(&parse_control, cb, _1, _2));
}




#if 0
boost::shared_ptr<ClientConnection> get_connection (const boost::asio::ip::tcp::endpoint &remote);

shared_ptr<ClientConnection>
ClientConnectionManager::get_connection (const tcp::endpoint &remote)
{
  map<tcp::endpoint, shared_ptr<ClientConnection> >::iterator iter = conns.find (remote);
  if (iter == conns.end())
    return shared_ptr<ClientConnection> ();
  else
    return iter->second;
}
#endif



