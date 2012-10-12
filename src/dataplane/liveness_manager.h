#ifndef _liveness_manager_H_
#define _liveness_manager_H_

#include "js_utils.h"
#include "node_config.h"
#include "connection.h"

namespace jetstream {

class LivenessManager {
 private:
  boost::shared_ptr<boost::asio::io_service> iosrv;
  NodeConfig &config;

  class ConnectionNotification {
   private:
    boost::shared_ptr<boost::asio::io_service> iosrv;
    boost::shared_ptr<ClientConnection> conn;
    NodeConfig &config;
    bool should_exit;
    boost::asio::deadline_timer timer;

    void wait_to_notify ();

   public:
    ConnectionNotification (boost::shared_ptr<boost::asio::io_service> srv,
			    boost::shared_ptr<ClientConnection> c,
			    NodeConfig& conf);

    ~ConnectionNotification();

    void send_notification (const boost::system::error_code &error);
    void stop_notify ();
    bool is_connected () const { return (conn && conn->is_connected()); }
    
  
    std::string conn_debug_str() {
      return (is_connected() ? conn->get_fourtuple() : " unbound connection ");
    }
    
  };

  // Map on connection four-tuple as key
  typedef std::map<std::string, 
    boost::shared_ptr<ConnectionNotification> > connection_map;
  connection_map connections;

 public:
  LivenessManager (boost::shared_ptr<boost::asio::io_service> srv,
		   NodeConfig& conf);

  void start_notifications (boost::shared_ptr<ClientConnection> c);
  void stop_all_notifications ();
};

}

#endif /* _liveness_manager_H_ */


