#ifndef _dataplaneoperator_H_
#define _dataplaneoperator_H_

#include <sys/types.h>
#include <map>

#include "js_utils.h"
#include "jetstream_types.pb.h"
#include "jetstream_dataplane.pb.h"

namespace jetstream {

class DataPlaneOperator {
 private:
  bool active;

 public:
  DataPlaneOperator() : active (false) {}
  ~DataPlaneOperator();
  virtual void execute();
};

typedef DataPlaneOperator *maker_t();
}

#endif /* _dataplaneoperator_H_ */
