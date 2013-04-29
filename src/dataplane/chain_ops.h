#ifndef __JetStream__chain_ops__
#define __JetStream__chain_ops__

#include "operator_chain.h"
#include "congest_policy.h"
#include <iostream>
#include <fstream>
#include "js_utils.h"

namespace jetstream {


class TimerSource: public COperator {
 public:
//  virtual operator_err_t configure(std::map<std::string,std::string> &config);
  virtual void start();
  virtual void stopping();  //called on strand


  virtual void set_congestion_policy(boost::shared_ptr<CongestionPolicy> p) {
    congest_policy = p;
  }
  
//  void end_of_window(msec_t duration);
  
  bool isRunning() {
    return running;
  }
  
  virtual bool is_source() {return true;}

  
  virtual void process(OperatorChain *, std::vector<boost::shared_ptr<Tuple> > &, DataplaneMessage&);
  
  virtual ~TimerSource();

  virtual void add_chain(boost::shared_ptr<OperatorChain> c) {chain = c;}

  
 protected:
  boost::shared_ptr<OperatorChain> chain;
  boost::shared_ptr<boost::asio::strand> st;
 
  TimerSource(): running(false),send_now(false),exit_at_end(true),ADAPT(true){}
  
  virtual int emit_data() = 0; //returns delay (in ms) for next send. negative means to stop sending.
  
  void emit_wrapper();


  boost::shared_ptr<CongestionPolicy> congest_policy;
  volatile bool running;
  volatile bool send_now, exit_at_end;
  
  bool ADAPT;
  boost::shared_ptr<boost::asio::deadline_timer> timer;
};


/***
 * Operator for emitting a specified number of generic tuples.
 */
class SendK: public TimerSource {
 public:
  virtual operator_err_t configure(std::map<std::string,std::string> &config);
  void reset() { n = 1; }

 protected:
  virtual int emit_data();
  boost::shared_ptr<Tuple> t;
  u_int64_t k, n;  // Number of tuples to send and number sent, respectively
  


GENERIC_CLNAME
};  


}

#endif /* defined(__JetStream__chain_ops__) */
