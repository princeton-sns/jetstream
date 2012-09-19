#include "connection.h"

#include <glog/logging.h>

using namespace std;
using namespace boost;
using namespace boost::asio::ip;
using namespace jetstream;


ClientConnection::ClientConnection (shared_ptr<asio::io_service> srv,
				    const tcp::endpoint &remoteEndpoint,
				    boost::system::error_code &error)
  : connected (false), iosrv (srv), sock (new tcp::socket(*iosrv)),
    remote (remoteEndpoint), timer (*iosrv)
{
  if (remote.address().is_v4())
    sock->open(tcp::v4(), error);
  else if (remote.address().is_v6())
    sock->open(tcp::v6(), error);
  else
    error = asio::error::address_family_not_supported;
  VLOG(1) << "Client connection via endpoint ctor";
}


ClientConnection::ClientConnection(boost::shared_ptr<ConnectedSocket> s)
  : connected (true), iosrv (s->get_iosrv()), sock(s->sock),
    remote (s->get_remote_endpoint()), timer (*iosrv),  
    connSock (s)
{
  VLOG(1) << "Client connection via connectedSocket ctor";
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
  connSock = cs;

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

  if (connSock)
    connSock->close();
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
  if (!connected || !connSock) {
    error = asio::error::not_connected;
    return;
  }

  connSock->send_msg(msg, error);
}


void
ClientConnection::recv_data_msg_cb (cb_data_protomsg_t cb, 
				    const SerializedMessageIn &msg,
				    const boost::system::error_code &error)
{
  DataplaneMessage dmsg;
  if (error)
    cb(dmsg, error);
  else {
    boost::system::error_code success;
    dmsg.ParseFromArray(msg.msg, msg.len);
    cb(dmsg, success);
  }
}


void
ClientConnection::recv_data_msg (cb_data_protomsg_t cb,
				 boost::system::error_code &error)
{
  if (!connected || !connSock) {
    error = asio::error::not_connected;
    return;
  }
  connSock->recv_msg(boost::bind(&ClientConnection::recv_data_msg_cb, cb, _1, _2));
}


void
ClientConnection::recv_control_msg_cb (cb_control_protomsg_t cb, 
				       const SerializedMessageIn &msg,
				       const boost::system::error_code &error)
{
  ControlMessage cmsg;
  if (error)
    cb(cmsg, error);
  else {
    boost::system::error_code success;
    cmsg.ParseFromArray(msg.msg, msg.len);
    cb(cmsg, success);
  }
}


void
ClientConnection::recv_control_msg (cb_control_protomsg_t cb,
				    boost::system::error_code &error)
{
  if (!connected || !connSock) {
    error = asio::error::not_connected;
    return;
  }
  connSock->recv_msg(boost::bind(&ClientConnection::recv_control_msg_cb, cb, _1, _2));
}

