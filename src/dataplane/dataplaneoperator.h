#ifndef _dataplaneoperator_H_
#define _dataplaneoperator_H_

#include <sys/types.h>
#include <map>
#include <iostream>

#include <boost/shared_ptr.hpp>
#include <boost/asio.hpp>


#include "js_utils.h"
#include "jetstream_types.pb.h"
#include "congestion_monitor.h"


//#include "node.h"


#define GENERIC_CLNAME  private: \
   const static std::string my_type_name; \
 public: \
   virtual const std::string& typename_as_str() {return my_type_name;}


namespace jetstream {

class Node;

class TupleReceiver {
 public:
  virtual void process (boost::shared_ptr<Tuple> t) = 0;
  virtual void no_more_tuples() = 0;
  virtual boost::shared_ptr<CongestionMonitor> congestion_monitor() {
    return boost::shared_ptr<CongestionMonitor>(new UncongestedMonitor);
  }

  virtual ~TupleReceiver() {}
  virtual const std::string& typename_as_str() = 0; //return a name for the type
  virtual std::string id_as_str() = 0;
    /** Return a longer description of the operator. Should NOT include the typename*/
  virtual std::string long_description() {return "";}

  
  virtual void meta_from_upstream(const DataplaneMessage & msg, const operator_id_t pred) = 0;

};

class TupleSender {
  public:
    virtual void meta_from_downstream(const DataplaneMessage & msg) = 0;
};

typedef std::map<std::string,std::string> operator_config_t;

typedef std::string operator_err_t;
const operator_err_t NO_ERR = "";

class DataPlaneOperator : public virtual TupleReceiver, public virtual TupleSender {
 private:
  operator_id_t operID; // note that id() returns a reference, letting us set this
  boost::shared_ptr<TupleReceiver> dest;
  boost::shared_ptr<TupleSender> pred;
  protected:   Node * node;  //NOT a shared pointer. Nodes always outlast their operators.

  private: int tuplesEmitted;

 protected:

  void emit (boost::shared_ptr<Tuple> t); // Passes the tuple along the chain
  virtual boost::shared_ptr<CongestionMonitor> congestion_monitor();
//  Node * get_node() {return node;} //not sure if we should allow operators this much access --asr
  
  boost::shared_ptr<boost::asio::deadline_timer> get_timer();
  
 public:
  DataPlaneOperator ():node(0),tuplesEmitted(0)  {}
  virtual ~DataPlaneOperator ();

  
  void set_dest (boost::shared_ptr<TupleReceiver> d) { dest = d; }
  
      //these need to be virtual to support the case where an operator has multiple preds
  virtual void add_pred (boost::shared_ptr<TupleSender> d) { pred = d; }
  virtual void clear_preds () { pred.reset(); }

  boost::shared_ptr<TupleReceiver> get_dest () { return dest; }
  
  

  /** This will be called before configure
  */
  void set_node (Node * n) { node = n; }
  
  
  /** A variety of (self-explanatory) debugging aids and metadata */
  operator_id_t & id() {return operID;}
  virtual std::string id_as_str() { return operID.to_string(); }
  int emitted_count() { return tuplesEmitted;}
  
    /** This method will be called on every operator, before start() and before
  * any tuples will be received. This method must not block or emit tuples
  */ 
  virtual operator_err_t configure (std::map<std::string, std::string> &)
      {return NO_ERR;}

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
   * This function must not block for long periods.
   * Once this method returns, the operator ID is no longer valid.
   * This method will be invoked by the io service threads, or by some other thread.
   * OPERATOR CODE SHOULD NOT CALL THIS ON A THREAD THEY MANAGE, because boost
   * doesn't let you join with yourself.
   */
  virtual void stop () {};
  
  
  virtual void meta_from_downstream(const DataplaneMessage &msg);
  
  virtual void meta_from_upstream(const DataplaneMessage & msg, const operator_id_t pred);

  
  GENERIC_CLNAME
};

typedef DataPlaneOperator *maker_t();


/* Group together the code for cleaning up operators. Could potentially fold this
back into Node
*/
class OperatorCleanup {
  
  public:
    OperatorCleanup(boost::asio::io_service& io):iosrv(io),cleanup_strand(iosrv) {}

      //called to invoke the stop method, BEFORE purging operator ID
    void stop_on_strand(boost::shared_ptr<DataPlaneOperator> op) {
       cleanup_strand.post( boost::bind(&DataPlaneOperator::stop, op) );

    }
    void cleanup(boost::shared_ptr<DataPlaneOperator> op);

  private:
    void cleanup_cb(boost::shared_ptr<DataPlaneOperator> op);

   boost::asio::io_service & iosrv;
   boost::asio::strand cleanup_strand;
};

}

#endif /* _dataplaneoperator_H_ */
