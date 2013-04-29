#ifndef __JetStream__chain_ops__
#define __JetStream__chain_ops__

#include "operator_chain.h"
#include "congest_policy.h"
#include <iostream>
#include <fstream>
#include "js_utils.h"

namespace jetstream {


#ifndef OP_TYPES
#define OP_TYPES
typedef std::map<std::string,std::string> operator_config_t;

typedef std::string operator_err_t;
const operator_err_t NO_ERR = "";
#endif



class COperator: virtual public ChainMember {

 public:
  virtual void process(OperatorChain * chain, std::vector<boost::shared_ptr<Tuple> > &, DataplaneMessage&) = 0;
  virtual ~COperator() {}
  virtual operator_err_t configure(std::map<std::string,std::string> &config) = 0;
  virtual void start() {}
//  virtual void stop() {} //called only on strand
  virtual bool is_source() {return false;}

  operator_id_t & id() {return operID;}
  virtual std::string id_as_str() { return operID.to_string(); }
  virtual const std::string& typename_as_str() = 0; //return a name for the type  
  virtual std::string long_description() {return "";}


  void set_node (Node * n) { node = n; }


  void unregister(); 

 protected:
    operator_id_t operID; // note that id() returns a reference, letting us set this
    Node * node;   
};



class CEachOperator: public COperator {

public:
  virtual void process(OperatorChain * chain,
                       std::vector<boost::shared_ptr<Tuple> > &,
                       DataplaneMessage&);
  
  virtual void process_one(boost::shared_ptr<Tuple> & t)  = 0;

};

class CFilterOperator: public COperator {

public:
  virtual void process(OperatorChain * chain,
                       std::vector<boost::shared_ptr<Tuple> > &,
                       DataplaneMessage&);
  
  virtual bool should_emit(const Tuple& t)  = 0;

};


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
