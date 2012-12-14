#ifndef _connection_H_
#define _connection_H_

#include <boost/asio.hpp>
#include <boost/thread.hpp>

#include "js_utils.h"
#include "jetstream_types.pb.h"
#include "connected_socket.h"

namespace jetstream {

class ClientConnection;

typedef boost::function<void (boost::shared_ptr<ClientConnection>,
			      const boost::system::error_code &) > cb_clntconn_t;

typedef boost::function<void (boost::shared_ptr<ConnectedSocket>,
			      const boost::system::error_code &) > cb_connsock_t;

typedef boost::function<void (const jetstream::DataplaneMessage &msg,
			      const boost::system::error_code &) > cb_data_protomsg_t;

typedef boost::function<void (const jetstream::ControlMessage &msg,
			      const boost::system::error_code &) > cb_control_protomsg_t;



class ServerConnection {
 private:
  bool accepting;
  boost::shared_ptr<boost::asio::io_service> iosrv;
  boost::asio::ip::tcp::acceptor srvAcceptor;
  boost::asio::ip::tcp::endpoint local;
  boost::asio::strand astrand;

  std::map<boost::asio::ip::tcp::endpoint, 
    boost::shared_ptr<ConnectedSocket> > clients;

  cb_connsock_t acceptcb;

  // Wrap do_accept and accepted in same strand
  void do_accept ();
  void accepted (boost::shared_ptr<boost::asio::ip::tcp::socket> newSock,
		 const boost::system::error_code &error);

 public:
  ServerConnection (boost::shared_ptr<boost::asio::io_service> srv,
		    const boost::asio::ip::tcp::endpoint &localEndpoing,
		    boost::system::error_code &error);
  ~ServerConnection ();

  const boost::asio::ip::tcp::endpoint & get_local_endpoint () const
  { return local; }

  void accept (cb_connsock_t cb, boost::system::error_code &error);
  bool is_accepting () const { return accepting; }
  void close ();
};


/**
 *  Represents a connection, either pending or already created. If and only if
 *   the connection is fully initialized, connSock will be defined.
 *
 *   sock itself will be defined regardless; it will be the same socket as that 
 *  wrapped by the connSock if the latter exists. 
 */
class ClientConnection {
 protected:
  bool connected;
  boost::shared_ptr<boost::asio::io_service> iosrv;
  boost::shared_ptr<boost::asio::ip::tcp::socket> sock;
  boost::asio::ip::tcp::endpoint remote;
  boost::asio::deadline_timer timer;

  boost::shared_ptr<ConnectedSocket> connSock;

  void connect_cb (cb_err_t cb, const boost::system::error_code &error);
  void timeout_cb (cb_err_t cb, const boost::system::error_code &error);


  static void recv_data_msg_cb (cb_data_protomsg_t cb, 
				const jetstream::SerializedMessageIn &msg,
				const boost::system::error_code &error);
  static void recv_control_msg_cb (cb_control_protomsg_t cb, 
				   const jetstream::SerializedMessageIn &msg,
				   const boost::system::error_code &error);

 public:
  ClientConnection (boost::shared_ptr<boost::asio::io_service> srv,
		    const boost::asio::ip::tcp::endpoint &remoteEndpoint,
		    boost::system::error_code &error);
  ClientConnection (boost::shared_ptr<ConnectedSocket> connSock);
  ~ClientConnection () { close(); }

  boost::asio::ip::tcp::endpoint get_remote_endpoint () const 
  { return remote; }
  boost::asio::ip::tcp::endpoint get_local_endpoint () const 
  { return connSock->get_local_endpoint (); }
  std::string get_fourtuple () const  //NOT SAFE TO CALL IF CONNECTION ISN'T UP
  { assert(connected);  return connSock->get_fourtuple(); }

  void connect (msec_t timeout, cb_err_t cb);
  bool is_connected () const
  { assert ( !connected || (connSock != NULL) ); return connected; }
  
  void close_async (close_cb_t);

  // Underlying use of async writes are thread safe
  void send_msg (const ProtobufMessage &msg,
		 boost::system::error_code &error);
  
  // void recv_msg (cb_protomsg_t cb, boost::system::error_code &error);
  void recv_data_msg (cb_data_protomsg_t cb, boost::system::error_code &error);
  void recv_control_msg (cb_control_protomsg_t cb, boost::system::error_code &error);

  size_t send_count() { return connSock->send_count(); }
  size_t bytes_queued() { return connSock->bytes_queued(); }
  boost::shared_ptr< QueueCongestionMonitor> congestion_monitor() {return connSock->congestion_monitor();}
  
  void close_now(); 

private:
  void close () {close_async(no_op_v);}

  
};



class ConnectionManager {
 private:
  boost::shared_ptr<boost::asio::io_service> iosrv;
  boost::asio::ip::tcp::resolver resolv;
  msec_t connTimeout; 

  void domain_resolved (cb_clntconn_t cb,
			const boost::system::error_code &error,
			boost::asio::ip::tcp::resolver::iterator resolved);

  void create_connection_cb (boost::asio::ip::tcp::resolver::iterator resolved,
			     boost::shared_ptr<ClientConnection> conn,
			     cb_clntconn_t cb,
			     const boost::system::error_code &error);
  
 public:
  ConnectionManager (boost::shared_ptr<boost::asio::io_service> srv,
		     msec_t connTmo = 10000)
    : iosrv (srv), resolv (*iosrv), connTimeout (connTmo) {}
  
  void create_connection (const std::string &domain, port_t port,
			  cb_clntconn_t cb);
  void create_connection (boost::asio::ip::tcp::resolver::iterator resolved,
			  cb_clntconn_t cb);
  
        
};


}

#endif /* _connection_H_ */


