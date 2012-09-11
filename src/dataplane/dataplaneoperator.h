#ifndef _dataplaneoperator_H_
#define _dataplaneoperator_H_

#include <sys/types.h>
#include <boost/shared_ptr.hpp>

#include "js_utils.h"
#include "jetstream_types.pb.h"
#include <map>

namespace jetstream {

struct operator_id_t {
  int32_t computation_id; // which computation
  int32_t task_id;        // which operator in the computation

  bool operator< (const operator_id_t& rhs) const {
    return computation_id < rhs.computation_id 
      || task_id < rhs.task_id;
  }
  
  std::string to_string () {
    std::ostringstream buf;
    buf << "(" << computation_id << "," << task_id << ")";
    return buf.str();
  }
    
  operator_id_t (int32_t c, int32_t t) : computation_id (c), task_id (t) {}
  operator_id_t () : computation_id (0), task_id (0) {}
};


class TupleReceiver {
 public:
  virtual void process (boost::shared_ptr<Tuple> t) = 0;
  virtual ~TupleReceiver() {}
};


class DataPlaneOperator : public TupleReceiver {
 private:
  int operID; // TODO: when is this set???  -Ari
  boost::shared_ptr<TupleReceiver> dest;

 protected:
  void emit (boost::shared_ptr<Tuple> t); // Passes the tuple along the chain
    
 public:
  DataPlaneOperator ()  {}
  virtual ~DataPlaneOperator ();

  virtual void process (boost::shared_ptr<Tuple> t); // NOT abstract here
  void set_dest (boost::shared_ptr<TupleReceiver> d) { dest = d; }

  /**
   * An operator must not start emitting tuples until start() has been called.
   * This function must not block. If asynchronous processing is required (e.g.,
   * in a source operator, launch a thread to do this).
   */
  virtual void start (std::map<std::string, std::string> config) {};

  /**
   * An operator should stop processing tuples before this returns.
   * This function must not block.
   */
  virtual void stop () {};
};

typedef DataPlaneOperator *maker_t();
}

#endif /* _dataplaneoperator_H_ */
