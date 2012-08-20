#ifndef _liveness_manager_H_
#define _liveness_manager_H_

#include "js_utils.h"
#include "connection.h"

namespace jetstream {

class LivenessManager {
 private:
  boost::shared_ptr<boost::asio::io_service> iosrv;
  msec_t heartbeat_time;

  class ConnectionNotification {
   private:
    boost::shared_ptr<boost::asio::io_service> iosrv;
    boost::shared_ptr<ClientConnection> conn;
    msec_t heartbeat_time;
    bool waiting;
    boost::asio::deadline_timer timer;

    void wait_to_notify ();

   public:
    ConnectionNotification (boost::shared_ptr<boost::asio::io_service> srv,
			    boost::shared_ptr<ClientConnection> c,
			    msec_t heartbeat);

    void send_notification (const boost::system::error_code &error);
    void stop_notify ();
    bool is_connected () const { return (conn && conn->is_connected()); }
  };
  
  typedef std::map<boost::asio::ip::tcp::endpoint,
    boost::shared_ptr<ConnectionNotification> > connection_map;
  connection_map connections;

 public:
  LivenessManager (boost::shared_ptr<boost::asio::io_service> srv,
		   msec_t heartbeat);

  void start_notifications (boost::shared_ptr<ClientConnection> c);
  void stop_all_notifications ();
};

}

#endif /* _liveness_manager_H_ */


