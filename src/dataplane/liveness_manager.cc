#include <boost/thread/thread.hpp>
#include <boost/thread/mutex.hpp>
#include <boost/date_time.hpp>

#include <glog/logging.h>

#include "jetstream_types.pb.h"
#include "liveness_manager.h"


using namespace std;
using namespace boost;
using namespace boost::asio::ip;
using namespace jetstream;

mutex _lm_mutex;

LivenessManager::LivenessManager (shared_ptr<asio::io_service> srv,
				  NodeConfig &conf)
  : iosrv (srv), config (conf)
{
}


void
LivenessManager::start_notifications (shared_ptr<ClientConnection> c)
{
  std::string fourtuple = c->get_fourtuple();

  connection_map::iterator iter = connections.find(fourtuple);
  if (iter != connections.end()) {
    // Stop existing notification
    iter->second->stop_notify();
    connections.erase (iter);
  }

  LOG(INFO) << "Starting notifications to " << fourtuple << endl;

  shared_ptr<ConnectionNotification> notif 
    (new ConnectionNotification (iosrv, c, config));

  connections[fourtuple] = notif;

  boost::system::error_code success; 
  notif->send_notification(success);
}


// Note: this method only cancels timers, it does not wait for them to complete their last
// notification. 
void
LivenessManager::stop_all_notifications ()
{
  for (connection_map::iterator iter = connections.begin();
       iter != connections.end(); ++iter) {
    iter->second->stop_notify ();
  }
  // Wait until the destructor for this, since canceled timers may asynchronously call
  // the handler one last time (see stop_notify()).
  // connections.clear ();
}


LivenessManager::ConnectionNotification::ConnectionNotification (boost::shared_ptr<boost::asio::io_service> srv,
								 boost::shared_ptr<ClientConnection> c,
								 NodeConfig &conf)
  : iosrv (srv), conn (c), config(conf), 
    shouldExit (false), timer (*iosrv)
{
  assert (conn);
}


void
LivenessManager::ConnectionNotification::send_notification (const boost::system::error_code &error)
{
  if (error || !is_connected()) {
    if (error != boost::system::errc::operation_canceled) {
    
      LOG(WARNING) << "Send notification "
       << conn_debug_str()
       << ": connected " << is_connected()
       << ": error " << error.message() << "(" << error.value() << ")" << endl;
    }
    return;
  }

  ControlMessage req;
  req.set_type(ControlMessage::HEARTBEAT);
  Heartbeat *h = req.mutable_heartbeat();
  h->set_cpuload_pct(0);
  h->set_freemem_mb(1000);
  NodeID *ep = h->mutable_dataplane_addr();
  // Assume the dataplane endpoint uses the same local address as the liveness
  // connection, but a different port specified in the config
  ep->set_address(conn->get_local_endpoint().address().to_string());
  assert(config.dataplane_ep.second != 0);
  ep->set_portno(config.dataplane_ep.second);
  
  boost::system::error_code send_error;
  conn->send_msg(req, send_error);

  if (send_error) {
    LOG(WARNING) << "Send error on " << conn->get_fourtuple()
	 << ": " << send_error.message() << endl;
  }
  else {
    LOG_EVERY_N(INFO, 20) << "Successfully scheduled message on "
	 << conn->get_fourtuple() << endl;
  }

  wait_to_notify();
}


void
LivenessManager::ConnectionNotification::wait_to_notify ()
{
  if (!is_connected() || shouldExit) {
    LOG(WARNING) << "Stopping wait_to_notify on " 
		 <<  conn_debug_str()
		 << ". Connected " << is_connected ()
		 << "; Should-exit: " << shouldExit << endl;
    return;
  }

  timer.expires_from_now(posix_time::milliseconds(config.heartbeat_time));
  timer.async_wait(bind(&LivenessManager::ConnectionNotification::send_notification, 
			this, _1));
}


void
LivenessManager::ConnectionNotification::stop_notify ()
{
  boost::system::error_code e;
  
  // This immediately schedules our handler with an error code, but not synchronously.
  timer.cancel(e);
  shouldExit = true;
}


LivenessManager::ConnectionNotification::~ConnectionNotification()
{
  // The timer should have been canceled by now, and there should be no possibility
  // of our handler being invoked.

  // This doesn't guarantee the above, but checks it with good probability.
  assert(shouldExit);
}
