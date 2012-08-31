#include "connection.h"
#include "jetstream_types.pb.h"

#include <glog/logging.h>

using namespace std;
using namespace boost;
using namespace boost::asio::ip;
using namespace jetstream;


void
ConnectedSocket::fail (const boost::system::error_code &error)
{
  LOG(WARNING) << "unexpected error in ConnectedSocket::fail: " << error.message() << endl;

  send_queue.clear();
  close();

  if (recv_cb) {
    SerializedMessageIn bad_msg (0);
    recv_cb (bad_msg, error);
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
  (const ProtobufMessage &m, boost::system::error_code &error)
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
ConnectedSocket::send_msg (const ProtobufMessage &m,
			   boost::system::error_code &error)
{
  shared_ptr<SerializedMessageOut> msg (new SerializedMessageOut (m, error));
  if (!error)
    send_strand.post(bind(&ConnectedSocket::perform_send, shared_from_this(), msg));
}


void
ConnectedSocket::perform_send (shared_ptr<SerializedMessageOut> msg)
{
  if (!sock->is_open())
    return;
  else if (sending) {
    VLOG(2) << "send is busy in perform_send, queueing" <<endl;
    send_queue.push_back(msg);
    send_strand.post(bind(&ConnectedSocket::perform_queued_send, 
			  shared_from_this()));
  }
  else {
    sending = true;
    VLOG(2) << "async send in perform_send" <<endl;
    // Keep hold of message until callback so not cleaned up until sent
    asio::async_write(*sock, 
		      asio::buffer(msg->msg, msg->nbytes),
		      send_strand.wrap(bind(&ConnectedSocket::sent, 
					    shared_from_this(), msg, _1, _2)));
  }
}

void
ConnectedSocket::perform_queued_send ()
{
  if (sending || send_queue.empty() || !sock->is_open())
    return;
  VLOG(2) << "queued send" <<endl;
  shared_ptr<SerializedMessageOut> msg = send_queue.front();
  send_queue.pop_front();

  sending = true;
  // Keep hold of message until callback so not cleaned up until sent
  asio::async_write(*sock, 
		    asio::buffer(msg->msg, msg->nbytes),
		    send_strand.wrap(bind(&ConnectedSocket::sent, 
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

  VLOG(2) << "successfully sent "<< bytes_transferred << " bytes" <<endl;
  if (!send_queue.empty())
    perform_queued_send();
}


/******************** Receiving messages ********************/

// Note: This method has to be thread safe.
void
ConnectedSocket::recv_msg (cb_raw_msg_t recvcb)
{
  recv_cb = recvcb;
  recv_strand.post(bind(&ConnectedSocket::perform_recv, shared_from_this()));
}


void
ConnectedSocket::perform_recv ()
{

  VLOG(2) << "In ConnectedSocket::perform_recv; port " << sock->local_endpoint().port();
  
  if (!sock->is_open() || receiving)
    return;


  receiving = true;

  // XXX Could also build vector of u_int8_t of size hdrlen, but then we need
  // to still ntohl() from 4 entries of vector.  So, embedding in code for now
  // that hdrlen is u_int32_t
  // // shared_ptr<vector<u_int32_t> > hdrbuf (new vector<u_int8_t> (hdrlen));
  assert(SerializedMessageOut::hdrlen == sizeof (u_int32_t));
  shared_ptr<u_int32_t > hdrbuf (new u_int32_t);

  asio::async_read(*sock,
		   asio::buffer( (void*) hdrbuf.get(), 4),
		   asio::transfer_at_least(SerializedMessageOut::hdrlen),
		   recv_strand.wrap(bind(&ConnectedSocket::received_header,
					 shared_from_this(), hdrbuf, _1, _2)));
}


void
ConnectedSocket::received_header (shared_ptr< u_int32_t > hdrbuf,
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
      || bytes_transferred != SerializedMessageOut::hdrlen)
    e = asio::error::invalid_argument;
  else if (!sock->is_open())
    e = asio::error::not_connected;
  else {
    msglen = ntohl (*hdrbuf);
    if (!msglen)
      e = asio::error::invalid_argument;
  }

  if (e) {
    receiving = false;
    fail(e);
    return;
  }
  VLOG(2) << "In ConnectedSocket::received_header; expecting " << msglen <<
      " data bytes on port " << sock->local_endpoint().port();

  shared_ptr<SerializedMessageIn> recv_msg (new SerializedMessageIn (msglen));

  boost::asio::async_read(*sock,
			  asio::buffer(recv_msg->msg, recv_msg->len),
			  asio::transfer_at_least(recv_msg->len),
			  recv_strand.wrap(bind(&ConnectedSocket::received_body,
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
    
    //re-enable callback
  if (sock->is_open()) {
    recv_strand.post(bind(&ConnectedSocket::perform_recv, shared_from_this()));

  }
  VLOG(2) << "In ConnectedSocket::received_body; passing along buffer of length "
     << recv_msg->len << " data bytes on port " << sock->local_endpoint().port();


  if (recv_cb) {
    boost::system::error_code success;  //FIXME: where do we set this?
    recv_cb(*recv_msg, success);
  }
}


