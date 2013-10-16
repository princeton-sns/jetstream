#ifndef __JetStream__operator_chain__
#define __JetStream__operator_chain__

#include <vector>
#include <boost/thread.hpp>
#include <string>
#include "jetstream_types.pb.h"
#include "js_utils.h"
#include <boost/asio.hpp>
#include "congestion_monitor.h"

namespace jetstream {

//class CSrcOperator;
class OperatorChain;
class Node;
typedef boost::function<void ()> close_cb_t;


#define GENERIC_CLNAME  private: \
   const static std::string my_type_name; \
 public: \
   virtual const std::string& typename_as_str() const {return my_type_name;}


class ChainMember {

  public:
   virtual void process(OperatorChain * chain, std::vector<boost::shared_ptr<Tuple> > &, DataplaneMessage&) = 0;
   virtual ~ChainMember() {}
   virtual bool is_source() = 0;
   virtual std::string id_as_str() const = 0;
   virtual const std::string& typename_as_str() const = 0; //return a name for the type
   virtual std::string long_description() const {return "";}
  
   virtual void add_chain(boost::shared_ptr<OperatorChain>) {}
   virtual void chain_stopping(OperatorChain * ) {}
   virtual boost::shared_ptr<CongestionMonitor> congestion_monitor() {
    return boost::shared_ptr<CongestionMonitor>(new UncongestedMonitor);   
   }
  
   virtual void meta_from_downstream(DataplaneMessage & msg) {};
  
  
};


class OperatorChain {

protected:
//  CSrcOperator * chain_head;
  std::vector< boost::shared_ptr<ChainMember> > ops;
  volatile bool running;
  mutable std::string cached_chain_name;


public:

  OperatorChain() : running(false), strand(NULL) {}
  ~OperatorChain();

  boost::asio::strand * strand;
  
  void start();
  void process(std::vector<boost::shared_ptr<Tuple> > &, DataplaneMessage&);
  void process(std::vector<boost::shared_ptr<Tuple> > & buf) {
    DataplaneMessage no_msg;
    process(buf, no_msg);
  }


  void meta_from_upstream (DataplaneMessage& m) {
    std::vector<boost::shared_ptr<Tuple> > dummy;
//    DataplaneMessage m2;
//    m2.CopyFrom(m);
    process(dummy, m);
  }

  void upwards_metadata(DataplaneMessage&, ChainMember * m);

  void stop();
  void stop_from_within() {
    do_stop(no_op_v);
    //unregister();
  }

  const std::string& chain_name() const;
  
  boost::shared_ptr<CongestionMonitor> congestion_monitor();
  
  
    //These methods are for structural changes
  
  void add_member(boost::shared_ptr<ChainMember> op = boost::shared_ptr<ChainMember>() ) {
    //default value is to push back a null pointer, just reserving a space
    ops.push_back(op);
  }

  unsigned members() const {
    return ops.size();
  }
  
  void set_start(boost::shared_ptr<ChainMember> op) {
    LOG_IF(FATAL, ops[0] != 0) << "should only call set-start when there's no existing start";
    ops[0] = op;
  }
  
  void clone_from(boost::shared_ptr<OperatorChain>);
  
  boost::shared_ptr<ChainMember> member(unsigned i) const {
    return ops[i];
  }

private:
  void stop_async(close_cb_t cb);
  void do_stop(close_cb_t);

//  void unregister(); //removes the chain from the source operator. This might result in a
      //garbage collect. Needed for upward-moving failures.
  
  void unblock(bool * stopped);
  boost::condition_variable chainStopped;
  boost::mutex stopwait_mutex;


};

}

#endif /* defined(__JetStream__operator_chain__) */
