#include "connection.h"
#include "jetstream_controlplane.pb.h"

using namespace std;
using namespace boost;
using namespace boost::asio::ip;
using namespace jetstream;


void
ConnectedSocket::fail (const boost::system::error_code &error)
{
  send_queue.clear();
  close();

  if (recv_cb) {
    // XXX Bad -- should be generic Request type
    ServerRequest req;
    req.set_type (ServerRequest::NOOP);
    recv_cb (req, error);
  }
}


void
ConnectedSocket::close ()
{
  if (sock->is_open()) {
    boost::system::error_code error;
    sock->cancel(error);
    sock->shutdown(tcp::socket::shutdown_both, error);
    sock->close(error);
  }
}


/******************** Sending messages ********************/

ConnectedSocket::SerializedMessageOut::SerializedMessageOut
  (const google::protobuf::Message &m, boost::system::error_code &error)
{
  size_t len_check = m.ByteSize();
  if (len_check > MAX_UINT32) {
    error = boost::asio::error::message_size;
    return;
  }

  u_int32_t len = lexical_cast<u_int32_t> (len_check);
  u_int32_t len_nbo = htonl (len);

  nbytes = len + hdrlen;
  msg = new u_int8_t[nbytes];

  memcpy(msg, &len_nbo, hdrlen);
  m.SerializeToArray((msg + hdrlen), len);
}


void
ConnectedSocket::send_msg (const google::protobuf::Message &m,
			   boost::system::error_code &error)
{
  shared_ptr<SerializedMessageOut> msg (new SerializedMessageOut (m, error));
  if (!error)
    astrand.post(bind(&ConnectedSocket::perform_send, shared_from_this(), msg));
}


void
ConnectedSocket::perform_send (shared_ptr<SerializedMessageOut> msg)
{
  if (!sock->is_open())
    return;
  else if (sending) {
    send_queue.push_back(msg);
    astrand.post(bind(&ConnectedSocket::perform_queued_send, 
		      shared_from_this()));
  }
  else {
    sending = true;
    // Keep hold of message until callback so not cleaned up until sent
    asio::async_write(*sock, 
		      asio::buffer(msg->msg, msg->nbytes),
		      astrand.wrap(bind(&ConnectedSocket::sent, 
					shared_from_this(), msg, _1, _2)));
  }
}

void
ConnectedSocket::perform_queued_send ()
{
  if (sending || send_queue.empty() || !sock->is_open())
    return;

  shared_ptr<SerializedMessageOut> msg = send_queue.front();
  send_queue.pop_front();

  sending = true;
  // Keep hold of message until callback so not cleaned up until sent
  asio::async_write(*sock, 
		    asio::buffer(msg->msg, msg->nbytes),
		    astrand.wrap(bind(&ConnectedSocket::sent, 
				      shared_from_this(), msg, _1, _2)));
}


void
ConnectedSocket::sent (shared_ptr<SerializedMessageOut> msg,
		       const boost::system::error_code &error,
		       size_t bytes_transferred)
{
  sending = false;

  // XXX Check for specific errors?
  if (error) {
    fail (error);
    return;
  }

  if (!send_queue.empty())
    perform_queued_send();
}


/******************** Receiving messages ********************/


void
ConnectedSocket::recv_msg (cb_protomsg_t recvcb) 
{
  recv_cb = recvcb;
  astrand.post(bind(&ConnectedSocket::perform_recv, shared_from_this()));
}


void
ConnectedSocket::perform_recv ()
{
  if (!sock->is_open())
    return;

  receiving = true;

  // XXX Could also build vector of u_int8_t of size hdrlen, but then we need
  // to still ntohl() from 4 entries of vector.  So, embedding in code for now
  // that hdrlen is u_int32_t
  // // shared_ptr<vector<u_int32_t> > hdrbuf (new vector<u_int8_t> (hdrlen));
  assert(SerializedMessageOut::hdrlen == sizeof (u_int32_t));
  shared_ptr<vector<u_int32_t> > hdrbuf (new vector<u_int32_t> (1));

  asio::async_read(*sock,
		   asio::buffer(*hdrbuf),
		   asio::transfer_at_least(SerializedMessageOut::hdrlen),
		   astrand.wrap(bind(&ConnectedSocket::received_header,
				     shared_from_this(), hdrbuf, _1, _2)));
}


void
ConnectedSocket::received_header (shared_ptr<vector<u_int32_t> > hdrbuf,
				  const boost::system::error_code &error,
				  size_t bytes_transferred)
{
  size_t msglen = 0;

  // XXX Differentiate between temp and permanent errors
  if (error) {
    receiving = false;
    fail(error);
    return;
  }

  boost::system::error_code e;
  if (!hdrbuf 
      || !(hdrbuf->size())
      || bytes_transferred != SerializedMessageOut::hdrlen)
    e = asio::error::invalid_argument;
  else if (!sock->is_open())
    e = asio::error::not_connected;
  else {
    msglen = ntohl((*hdrbuf)[0]);
    if (!msglen)
      e = asio::error::invalid_argument;
  }

  if (e) {
    receiving = false;
    fail(e);
    return;
  }


  shared_ptr<SerializedMessageIn> recv_msg (new SerializedMessageIn (msglen));

  boost::asio::async_read(*sock,
			  asio::buffer(recv_msg->msg, recv_msg->len),
			  asio::transfer_at_least(recv_msg->len),
			  astrand.wrap(bind(&ConnectedSocket::received_body,
					    shared_from_this(), recv_msg, _1, _2)));
}


void
ConnectedSocket::received_body (shared_ptr<SerializedMessageIn> recv_msg,
				const boost::system::error_code &error,
				size_t bytes_transferred)

{
  receiving = false;

  // XXX Differentiate between temp and permanent errors
  if (error) {
    fail(error);
    return;
  }
  else if (!recv_msg || bytes_transferred != recv_msg->len) {
    fail(asio::error::invalid_argument);
    return;
  }
    
  if (sock->is_open())
    astrand.post(bind(&ConnectedSocket::perform_recv, shared_from_this()));

  if (recv_cb) {
    boost::system::error_code success;
    ServerRequest req;
    req.set_type (ServerRequest::NOOP);
    req.ParseFromArray(recv_msg->msg, recv_msg->len);
    recv_cb(req, success);
  }
}


