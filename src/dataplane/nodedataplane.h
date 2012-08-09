#ifndef _nodedataplane_H_
#define _nodedataplane_H_

#include <sys/types.h>

#include "js_utils.h"
#include "jetstream_types.pb.h"
#include "jetstream_dataplane.pb.h"
#include "workerclient.h"


namespace jetstream {
  
  class net_interface;
  
  class hb_loop {
    WorkerClient* uplink;
  public:
    hb_loop(WorkerClient* t):uplink(t) {}
    //could potentially add a ctor here with some args
    void operator()();
  };
  
  
  class ConnectionToController: public WorkerClient {
  public:
    ConnectionToController(boost::asio::io_service& io_service,
                           tcp::resolver::iterator endpoint_iterator):
    WorkerClient(io_service,endpoint_iterator) {}
    virtual void processMessage(protobuf::Message &msg);
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
