#ifndef _nodedataplane_H_
#define _nodedataplane_H_

#include <sys/types.h>

#include "js_utils.h"
#include "jetstream_types.pb.h"
#include "jetstream_dataplane.pb.h"

namespace jetstream {

  class net_interface;
  
  class hb_loop {
    net_interface* iface;
  public:
    hb_loop(net_interface* t):iface(t) {}
    //could potentially add a ctor here with some args
    void operator()();
  };
  
class NodeDataPlane {
 private:
  bool alive;
  net_interface* iface;

 public:
  NodeDataPlane() : alive (false) {}
  ~NodeDataPlane();
  void start_heartbeat_thread();
  
};

  const int HB_INTERVAL = 5; //seconds
  
}


#endif /* _nodedataplane_H_ */
