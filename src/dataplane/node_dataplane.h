#ifndef _nodedataplane_H_
#define _nodedataplane_H_

#include <sys/types.h>

#include "js_utils.h"
#include "jetstream_types.pb.h"
#include "jetstream_dataplane.pb.h"
#include "worker_conn_handler.h"


namespace jetstream {
  
class net_interface;
  
class ConnectionToController : public WorkerConnHandler {
 public:
  ConnectionToController (boost::asio::io_service& io_service,
			  boost::asio::ip::tcp::resolver::iterator endpoint_iterator)
    : WorkerConnHandler (io_service, endpoint_iterator) {}

  virtual ~ConnectionToController () {}
  virtual void process_message (char *buf, size_t sz);
};
  
  
class hb_loop {
 private:
  boost::shared_ptr<ConnectionToController> uplink;
 public:
  hb_loop (boost::shared_ptr<ConnectionToController> t) : uplink (t) {}
  //could potentially add a ctor here with some args
  void operator () ();
};
  

class NodeDataPlane {
 private:
  bool alive;
  boost::shared_ptr<boost::asio::io_service> iosrv;
  boost::shared_ptr<ConnectionToController> uplink;
  //ClientConnectionPool pool;

 public:
  NodeDataPlane();
  ~NodeDataPlane();
  void connect_to_master ();
  void start_heartbeat_thread();
  
};

const int HB_INTERVAL = 5; //seconds
  
}


#endif /* _nodedataplane_H_ */
