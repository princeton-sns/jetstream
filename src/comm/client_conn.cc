#include "client_conn.h"

using namespace std;
using namespace boost;
using namespace boost::asio::ip;
using namespace jetstream;


void
ClientConnectionManager::create_connection (const string &domain, port_t port,
					    bfunc_clntconn cb)
{
  string portstr = lexical_cast<string> (port);

  boost::system::error_code error;
  address addr = address::from_string(domain, error);

  if (!error) {
    // Domain supplied was a valid IP address
    tcp::resolver::iterator dests 
      = tcp::resolver::iterator::create(tcp::endpoint(addr, port), 
					domain, portstr);
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
					  const boost::system::error_code &error,
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

  boost::system::error_code error;
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
					       const boost::system::error_code &error)
{
  if (!error)
    cb(conn, error);
  else
    create_connection(dests, cb);
}


ClientConnection::ClientConnection (shared_ptr<asio::io_service> srv,
				    const tcp::endpoint &d,
				    boost::system::error_code &error)
  : connected (false), iosrv (srv), astrand (*srv),
    dest (d), sock (*iosrv), timer (*iosrv), writing (false)
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
			      const boost::system::error_code &error)
{
  // Unregister timeout
  boost::system::error_code e;
  timer.cancel(e);

  if (error)
    close(e);
  else
    connected = true;

  cb(error);
}


void
ClientConnection::timeout_cb(bfunc_err cb,
			     const boost::system::error_code &error)
{
  if (error == asio::error::operation_aborted)
    return;

  boost::system::error_code e;
  close(e);

  cb(asio::error::timed_out);
}
  

void 
ClientConnection::close (boost::system::error_code &error)
{
  connected = false;

  if (sock.is_open())
    sock.shutdown(tcp::socket::shutdown_both, error);
  
  sock.close (error);
}


/******************** Writing messages ********************/

ClientConnection::SerializedMessage::SerializedMessage (const google::protobuf::Message &m,
							system::error_code &error)
{
  size_t len_check = m.ByteSize();
  if (len_check > MAX_UINT32) {
    error = boost::asio::error::message_size;
    return;
  }

  u_int32_t len = lexical_cast<u_int32_t> (len_check);
  u_int32_t len_nbo = htonl (len);

  nbytes = len + sizeof(u_int32_t);
  msg = new u_int8_t[nbytes];

  memcpy(msg, &len_nbo, sizeof(u_int32_t));
  m.SerializeToArray((msg + sizeof(u_int32_t)), len);
}


void
ClientConnection::write_msg (const google::protobuf::Message &m,
			     system::error_code &error)
{
  shared_ptr<SerializedMessage> msg (new SerializedMessage (m, error));
  if (!error)
    astrand.post(bind(&ClientConnection::perform_write, this, msg));
}


void
ClientConnection::perform_write (shared_ptr<SerializedMessage> msg)
{
  // Must call function from a single strand to ensure single writer to
  // write_queue at a time
  if (writing) {
    write_queue.push_back(msg);
    astrand.post(bind(&ClientConnection::perform_queued_write, this));
  }
  else {
    writing = true;
    // Keep hold of message until callback so not cleaned up until sent
    asio::async_write(sock, 
		      asio::buffer(msg->msg, msg->nbytes),
		      astrand.wrap(bind(&ClientConnection::wrote, 
					this, msg, _1, _2)));
  }
}

void
ClientConnection::perform_queued_write ()
{
  if (writing || write_queue.empty())
    return;

  shared_ptr<SerializedMessage> msg = write_queue.front();
  write_queue.pop_front();

  writing = true;
  // Keep hold of message until callback so not cleaned up until sent
  asio::async_write(sock, 
		    asio::buffer(msg->msg, msg->nbytes),
		    astrand.wrap(bind(&ClientConnection::wrote, 
				      this, msg, _1, _2)));
}


void
ClientConnection::wrote (shared_ptr<SerializedMessage> msg,
			 const system::error_code &error,
			 size_t bytes_transferred)
{
  writing = false;
  // XXX Check for specific errors?
  if (error) {
    system::error_code e;
    close (e);
    return;
  }

  if (!write_queue.empty())
    perform_queued_write();
}


/******************** Reading messages ********************/


void 
ClientConnection::read_msg (google::protobuf::Message &msg,
			    boost::system::error_code &error)
{

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



