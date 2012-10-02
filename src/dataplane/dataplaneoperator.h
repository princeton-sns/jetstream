#ifndef _dataplaneoperator_H_
#define _dataplaneoperator_H_

#include <sys/types.h>
#include <boost/shared_ptr.hpp>

#include "js_utils.h"
#include "jetstream_types.pb.h"
#include <map>
#include <iostream>

//#include "node.h"

namespace jetstream {

class Node;


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

inline std::ostream& operator<<(std::ostream& out, operator_id_t id) {
  out << "(" << id.computation_id << "," << id.task_id << ")";
  return out;
}

class TupleReceiver {
 public:
  virtual void process (boost::shared_ptr<Tuple> t) = 0;
  virtual void no_more_tuples() = 0;
  
  virtual ~TupleReceiver() {}
  virtual const std::string& typename_as_str() = 0; //return a name for the type
  virtual std::string id_as_str() = 0;
    /** Return a longer description of the operator. Should NOT include the typename*/
  virtual std::string long_description() {return "";}
  

  
};

typedef std::map<std::string,std::string> operator_config_t;



class DataPlaneOperator : public TupleReceiver {
 private:
  operator_id_t operID; // note that id() returns a reference, letting us set this
  boost::shared_ptr<TupleReceiver> dest;
  Node * node;  //NOT a shared pointer. Nodes always outlast their operators.
  const static std::string my_type_name;
  int tuplesEmitted;

 protected:
  void emit (boost::shared_ptr<Tuple> t); // Passes the tuple along the chain
    
 public:
  DataPlaneOperator ():tuplesEmitted(0)  {}
  virtual ~DataPlaneOperator ();

  
  void set_dest (boost::shared_ptr<TupleReceiver> d) { dest = d; }
  void set_node (Node * n) { node = n; }
  boost::shared_ptr<TupleReceiver> get_dest () { return dest; }
  
  
  /** A variety of (self-explanatory) debugging aids and metadata */
  operator_id_t & id() {return operID;}
  virtual std::string id_as_str() { return operID.to_string(); }
  virtual const std::string& typename_as_str() { return my_type_name; }
  int emitted_count() { return tuplesEmitted;}
  
    /** This method will be called on every operator, before start() and before
  * any tuples will be received. This method must not block or emit tuples
  */ 
  virtual void configure (std::map<std::string, std::string> &) {};

  /**
   * An operator must not start emitting tuples until start() has been called or
   * until it has received a tuple.
   * This function ought not block. If asynchronous processing is required (e.g.,
   * in a source operator, launch a thread to do this).
   * Special dispensation for test code.
   */
  virtual void start () {};
  
  
  
  virtual void process (boost::shared_ptr<Tuple> t); // NOT abstract here
  
  /** Called when no more data will be passed in from upstream; operators
  may choose to stop in response to this.
  */
  virtual void no_more_tuples();

  /**
   * An operator should stop processing tuples before this returns.
   * This function must not block.
   */
  virtual void stop () {};
};

typedef DataPlaneOperator *maker_t();
}

#endif /* _dataplaneoperator_H_ */
