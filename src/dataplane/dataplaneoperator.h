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
#include "congest_policy.h"
#include <glog/logging.h>


//#include "node.h"


#define GENERIC_CLNAME  private: \
   const static std::string my_type_name; \
 public: \
   virtual const std::string& typename_as_str() {return my_type_name;}


namespace jetstream {

class Node;
class TupleReceiver;

class TupleSender {
  public:
    virtual void meta_from_downstream(const DataplaneMessage & msg) = 0;
    virtual void chain_is_broken() = 0;
    virtual std::string id_as_str() = 0;
  
  protected:
    boost::shared_ptr<TupleReceiver> dest;
  
  
};

class TupleReceiver {
 public:
  virtual void process (boost::shared_ptr<Tuple> t, const operator_id_t pred) = 0;

  /*
    Note that the dimension fields of oldV need not be defined.
  */
  virtual void process_delta (Tuple& oldV, boost::shared_ptr<Tuple> newV, const operator_id_t pred) = 0;
  
  virtual boost::shared_ptr<CongestionMonitor> congestion_monitor() {
    return boost::shared_ptr<CongestionMonitor>(new UncongestedMonitor);
  }

  virtual ~TupleReceiver() {}
  virtual const std::string& typename_as_str() = 0; //return a name for the type
  virtual std::string id_as_str() = 0;
    /** Return a longer description of the operator. Should NOT include the typename*/
  virtual std::string long_description() {return "";}

  virtual void meta_from_upstream(const DataplaneMessage & msg, const operator_id_t pred) = 0;

      //these need to be virtual to support the case where an operator has multiple preds
  virtual void add_pred (boost::shared_ptr<TupleSender> d) { pred = d; }
  virtual void clear_preds () { pred.reset(); }
  virtual void remove_pred ( operator_id_t id) { pred.reset(); }


protected:
  boost::shared_ptr<TupleSender> pred;


};

#ifndef OP_TYPES
#define OP_TYPES
typedef std::map<std::string,std::string> operator_config_t;

typedef std::string operator_err_t;
const operator_err_t NO_ERR = "";
#endif

class DataPlaneOperator : public virtual TupleReceiver, public virtual TupleSender {
  friend class OperatorCleanup;

 private:
  operator_id_t operID; // note that id() returns a reference, letting us set this
  protected:   Node * node;  //NOT a shared pointer. Nodes always outlast their operators.

  private: int tuplesEmitted;

 protected:

  void emit (boost::shared_ptr<Tuple> t); // Passes the tuple along the chain
  void emit (Tuple& old, boost::shared_ptr<Tuple>); // Passes a delta along the chain

  void send_meta_downstream(const DataplaneMessage & msg);
  
//  Node * get_node() {return node;} //not sure if we should allow operators this much access --asr
  
  boost::shared_ptr<boost::asio::deadline_timer> get_timer();
  
 public:
  DataPlaneOperator ():node(0),tuplesEmitted(0)  {}
  virtual ~DataPlaneOperator ();
  virtual boost::shared_ptr<CongestionMonitor> congestion_monitor();

  
  void set_dest (boost::shared_ptr<TupleReceiver> d) { dest = d; }

  boost::shared_ptr<TupleReceiver> get_dest () { return dest; }
  
  

  /** This will be called before configure
  */
  void set_node (Node * n) { node = n; }
  
  virtual void set_congestion_policy(boost::shared_ptr<CongestionPolicy> p) {}
  
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
  virtual void process (boost::shared_ptr<Tuple> t, const operator_id_t src); // NOT abstract here
  
  virtual void process_delta (Tuple& oldV, boost::shared_ptr<Tuple> newV, const operator_id_t pred);
  
  
  /** Called when no more data will be passed in from upstream; operators
  may choose to stop in response to this.
  */
  virtual void no_more_tuples();

  /**
   * An operator should stop processing tuples before this returns.
   * This function must not block for long periods.
   * Once this method returns, the operator ID is no longer valid.
   * This method will be invoked by the io service threads, or by some other thread.
   * OPERATOR CODE SHOULD NOT CALL THIS INSIDE A THREAD THEY MANAGE, because boost
   * doesn't let you join with yourself.
   */
  virtual void stop () {
    VLOG(1) << "stop() for base DataPlaneOperator " << id();
  };
  
  virtual void chain_is_broken();
  
  virtual void meta_from_downstream(const DataplaneMessage &msg);
  
  virtual void meta_from_upstream(const DataplaneMessage & msg, const operator_id_t pred);

private:
  
  GENERIC_CLNAME
};

/* Group together the code for cleaning up operators. Could potentially fold this
back into Node
*/
class OperatorCleanup {
  
  public:
    OperatorCleanup(boost::asio::io_service& io):iosrv(io),cleanup_strand(iosrv) {
    }

      //called to invoke the stop method, BEFORE purging operator ID
    void stop_on_strand(boost::shared_ptr<DataPlaneOperator> op) {
      VLOG(1) << " stopping " << op->id() << " on strand";
      cleanup_strand.post( boost::bind(&DataPlaneOperator::stop, op) );
    }
    void cleanup(boost::shared_ptr<DataPlaneOperator> op);

  private:
    void cleanup_cb(boost::shared_ptr<DataPlaneOperator> op);

   boost::asio::io_service & iosrv;
   boost::asio::strand cleanup_strand;
};


class xDummyReceiver: public DataPlaneOperator {
 public:
  std::vector< boost::shared_ptr<Tuple> > tuples;
  bool store;

  virtual void process(boost::shared_ptr<Tuple> t) {
    if(store)
      tuples.push_back(t);
  }
 
  virtual operator_err_t configure (std::map<std::string, std::string> & config){
    if (config["no_store"].length() > 0)
      store=false;
    return NO_ERR;
  }


  virtual void process_delta (Tuple& oldV, boost::shared_ptr<Tuple> newV, const operator_id_t pred) {}

  
  virtual std::string long_description() {
      std::ostringstream buf;
      buf << tuples.size() << " stored tuples.";
      return buf.str();
  }
  
  virtual void no_more_tuples() {} //don't exit at end; keep data available
  
  virtual ~xDummyReceiver() {}
  xDummyReceiver(): store(true) {}
  

GENERIC_CLNAME
};



class RateRecordReceiver: public DataPlaneOperator {

 protected:
  volatile bool running;

  boost::mutex mutex;
  
  boost::posix_time::ptime window_start;
  
  volatile long tuples_in_window;
  volatile long bytes_in_window;

  double bytes_per_sec;
  double tuples_per_sec;

  boost::shared_ptr<boost::thread> loopThread;


 public:
   RateRecordReceiver():
     running(false), tuples_in_window(0),bytes_in_window(0) {}
 
  virtual void process(boost::shared_ptr<Tuple> t);
  
  virtual std::string long_description();
  
  virtual void no_more_tuples() {} //don't exit at end; keep data available
  
  virtual void start();
  virtual void stop();
  void operator()();  // A thread that will loop while reading the file    


GENERIC_CLNAME
};


/***
 * Operator for artificially imposing congestion.
 */
class MockCongestion: public DataPlaneOperator {
 public:
  virtual void process(boost::shared_ptr<Tuple> t) {
    emit(t);
  }
  virtual boost::shared_ptr<CongestionMonitor> congestion_monitor() {
    return mon;
  }

  MockCongestion();

  double congestion;

private:
  boost::shared_ptr<CongestionMonitor> mon;

GENERIC_CLNAME
};  


class CountLogger: public DataPlaneOperator {
  //logs the total counts going past
 public:

  CountLogger(): tally_in_window(0),field(0) {}
  virtual void process(boost::shared_ptr<Tuple> t);
  virtual operator_err_t configure(std::map<std::string,std::string> &config);
  virtual void meta_from_upstream(const DataplaneMessage & msg, const operator_id_t pred);

 private:
  unsigned tally_in_window;
  unsigned field;


GENERIC_CLNAME
};  



}

#endif /* _dataplaneoperator_H_ */
