#ifndef _dataplaneoperator_H_
#define _dataplaneoperator_H_

#include <sys/types.h>
#include <map>

#include "js_utils.h"
#include "jetstream_types.pb.h"
#include "jetstream_dataplane.pb.h"

typedef void* tuple_t; //FIXME when we have a real tuple

namespace jetstream {

class Receiver {
 public:
  virtual void process(tuple_t) = 0;
};

class DataPlaneOperator:Receiver {
 private:
  bool active;
  int operID;
  Receiver * dest;
  
 protected:
  void emit(tuple_t); //
      
 public:
 
  DataPlaneOperator() : active (false), dest(NULL) {}
  virtual ~DataPlaneOperator();
  virtual void process(tuple_t); //NOT abstract here
};

typedef DataPlaneOperator *maker_t();
}

#endif /* _dataplaneoperator_H_ */
