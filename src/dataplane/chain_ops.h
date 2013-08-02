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
  
  COperator():node(0) {}
  
  virtual void process(OperatorChain * chain, std::vector<boost::shared_ptr<Tuple> > &, DataplaneMessage&) = 0;
  virtual ~COperator(); 
  virtual operator_err_t configure(std::map<std::string,std::string> &config) = 0;
  virtual void start() {} //NOTE: This is only called on source operators!
  virtual bool is_source() {return false;}
  virtual bool is_chain_end() {return false;}

  const operator_id_t & id() const {return operID;}
  virtual std::string id_as_str() const { return operID.to_string(); }

  virtual void set_congestion_policy(boost::shared_ptr<CongestionPolicy>) {}
  void set_node (Node * n) { node = n; }
  void set_id (const operator_id_t & i) { operID = i; }


//  void unregister();

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
  virtual void meta_from_upstream(OperatorChain * c, DataplaneMessage & msg) {}
  

};

class CFilterOperator: public COperator {

public:
  virtual void process(OperatorChain * chain,
                       std::vector<boost::shared_ptr<Tuple> > &,
                       DataplaneMessage&);
  
  virtual bool should_emit(const Tuple& t)  = 0;

  virtual void end_of_window( DataplaneMessage & msg) {};

};


class TimerSource: public COperator {
 public:
//  virtual operator_err_t configure(std::map<std::string,std::string> &config);
  virtual void start();
  virtual void chain_stopping(OperatorChain *);  //called on strand


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


}

#endif /* defined(__JetStream__chain_ops__) */
