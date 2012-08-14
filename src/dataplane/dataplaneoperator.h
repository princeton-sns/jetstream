#ifndef _dataplaneoperator_H_
#define _dataplaneoperator_H_

#include <sys/types.h>
#include <boost/shared_ptr.hpp>

#include "js_utils.h"
#include "jetstream_types.pb.h"
#include "jetstream_dataplane.pb.h"
#include <map>

namespace jetstream {

using namespace std;
using namespace boost;
using namespace edu::princeton::jetstream;
  
class Receiver {
 public:
  virtual void process(shared_ptr<Tuple> t) = 0;
};

class DataPlaneOperator:public Receiver  {
 private:
  int operID; //TODO: when is this set???  -Ari
  Receiver * dest;

 protected:
  void emit(shared_ptr<Tuple>  t); //passes the tuple along the chain
    
 public:
  void set_dest(Receiver* d) {dest = d;}
  DataPlaneOperator() : dest(NULL) {}
  virtual ~DataPlaneOperator();
  virtual void process(shared_ptr<Tuple> t); //NOT abstract here

  /**
   * An operator must not start emitting tuples until start() has been called.
   */
  virtual void start(map<string,string> config) {};

  /**
   *
   */
  virtual void stop() {};
};

typedef DataPlaneOperator *maker_t();
}

#endif /* _dataplaneoperator_H_ */
