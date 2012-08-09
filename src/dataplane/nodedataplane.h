#ifndef _nodedataplane_H_
#define _nodedataplane_H_

#include <sys/types.h>

#include "js_utils.h"
#include "jetstream_types.pb.h"
#include "jetstream_dataplane.pb.h"
#include "worker_conn_handler.h"


namespace jetstream {
  
  class net_interface;

  
  class ConnectionToController: public WorkerConnHandler {
  public:
    ConnectionToController(boost::asio::io_service& io_service,
                           tcp::resolver::iterator endpoint_iterator):
    WorkerConnHandler(io_service,endpoint_iterator) {}
    virtual void processMessage(protobuf::Message &msg);
  };
  
  
  class hb_loop {
    ConnectionToController* uplink;
  public:
    hb_loop(ConnectionToController* t):uplink(t) {}
    //could potentially add a ctor here with some args
    void operator()();
  };
  
  
  class NodeDataPlane {
   private:
    bool alive;
    ConnectionToController* uplink;

   public:
    NodeDataPlane() : alive (false) {}
    ~NodeDataPlane();
    void connect_to_master();
    void start_heartbeat_thread();
    
  };

  const int HB_INTERVAL = 5; //seconds
  
}


#endif /* _nodedataplane_H_ */
