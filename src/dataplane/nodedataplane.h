#ifndef _nodedataplane_H_
#define _nodedataplane_H_

#include <sys/types.h>

#include "js_utils.h"
#include "jetstream_types.pb.h"
#include "jetstream_dataplane.pb.h"

namespace jetstream {

  
  class hb_loop {
    
  public:
    hb_loop() {}
    //could potentially add a ctor here with some args
    void operator()();
  };
  
class NodeDataPlane {
 private:
  bool alive;

 public:
  NodeDataPlane() : alive (false) {}
  ~NodeDataPlane();
  void start_heartbeat_thread();
  
};

  
}


#endif /* _nodedataplane_H_ */
