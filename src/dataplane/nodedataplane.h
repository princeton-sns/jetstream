#ifndef _nodedataplane_H_
#define _nodedataplane_H_

#include <sys/types.h>

#include "js_utils.h"
#include "jetstream_types.pb.h"
#include "jetstream_dataplane.pb.h"

namespace jetstream {

class NodeDataPlane {
 private:
  bool alive;

 public:
  NodeDataPlane() : alive (false) {}
  ~NodeDataPlane();
  
};

}

#endif /* _nodedataplane_H_ */
