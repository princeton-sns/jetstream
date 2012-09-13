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
				  msec_t heartbeat)
  : iosrv (srv), heartbeat_time (heartbeat)
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
    (new ConnectionNotification (iosrv, c, heartbeat_time));

  connections[fourtuple] = notif;

  boost::system::error_code success; 
  notif->send_notification(success);
}


void
LivenessManager::stop_all_notifications ()
{
  for (connection_map::iterator iter = connections.begin();
       iter != connections.end(); ++iter) {
    iter->second->stop_notify ();
  }
  connections.clear ();
}


LivenessManager::ConnectionNotification::ConnectionNotification (boost::shared_ptr<boost::asio::io_service> srv,
								 boost::shared_ptr<ClientConnection> c,
								 msec_t heartbeat)
  : iosrv (srv), conn (c), heartbeat_time (heartbeat), 
    waiting (false), timer (*iosrv)
{
  assert (conn);
}


void
LivenessManager::ConnectionNotification::send_notification (const boost::system::error_code &error)
{
  if (error || !is_connected()) {
    LOG(WARNING) << "Send notification on "
		 << conn->get_fourtuple()
		 << ": connected " << is_connected()
		 << ": error " << error.message() << endl;
    return;
  }
    
  waiting = false;

  ControlMessage req;
  req.set_type(ControlMessage::HEARTBEAT);
  Heartbeat *h = req.mutable_heartbeat();
  h->set_cpuload_pct(0);
  h->set_freemem_mb(1000);
  
  boost::system::error_code send_error;
  conn->send_msg(req, send_error);

  if (send_error) {
    LOG(WARNING) << "Send error on " << conn->get_fourtuple()
	 << ": " << send_error.message() << endl;
  }
  else {
    LOG(INFO) << "Successfully scheduled message on "
	 << conn->get_fourtuple() << endl;
  }

  wait_to_notify();
}


void
LivenessManager::ConnectionNotification::wait_to_notify ()
{
  if (!is_connected() || waiting) {
    LOG(WARNING) << "Stopping wait_to_notify on " 
		 << conn->get_fourtuple()
		 << ". Connected " << is_connected ()
		 << "; Waiting " << waiting << endl;
    return;
  }

  waiting = true;
  timer.expires_from_now(posix_time::milliseconds(heartbeat_time));
  timer.async_wait(bind(&LivenessManager::ConnectionNotification::send_notification, 
			this, _1));
}


void
LivenessManager::ConnectionNotification::stop_notify ()
{
  boost::system::error_code e;
  // This immediately schedules our handler with an error code, but not synchronously.
  // Since the handler will just bail anyway, don't wait and reset 'waiting' here.
  timer.cancel(e);
  waiting = false;
}

LivenessManager::ConnectionNotification::~ConnectionNotification()
{
  stop_notify();
}
