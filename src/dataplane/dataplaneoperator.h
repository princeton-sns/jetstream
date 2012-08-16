#ifndef _dataplaneoperator_H_
#define _dataplaneoperator_H_

#include <sys/types.h>
#include <boost/shared_ptr.hpp>

#include "js_utils.h"
#include "jetstream_types.pb.h"
#include "jetstream_dataplane.pb.h"
#include <map>

namespace jetstream {

class Receiver {
 public:
  virtual void process (boost::shared_ptr<Tuple> t) = 0;
};

class DataPlaneOperator : public Receiver {
 private:
  int operID; //TODO: when is this set???  -Ari
  boost::shared_ptr<Receiver> dest;

 protected:
  void emit(boost::shared_ptr<Tuple> t); //passes the tuple along the chain
    
 public:
  DataPlaneOperator ()  {}
  virtual ~DataPlaneOperator ();

  virtual void process (boost::shared_ptr<Tuple> t); //NOT abstract here
  void set_dest (boost::shared_ptr<Receiver> d) {dest = d;}

  /**
   * An operator must not start emitting tuples until start() has been called.
   */
  virtual void start (std::map<std::string, std::string> config) {};

  /**
   *
   */
  virtual void stop () {};
};

typedef DataPlaneOperator *maker_t();
}

#endif /* _dataplaneoperator_H_ */
