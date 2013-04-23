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
  virtual void stop();

  virtual void set_congestion_policy(boost::shared_ptr<CongestionPolicy> p) {
    congest_policy = p;
  }
  
  virtual bool is_source() {return true;}  
  
//  void end_of_window(msec_t duration);
  
  bool isRunning() {
    return running;
  }
  
  virtual void process(OperatorChain *, std::vector<boost::shared_ptr<Tuple> > &, DataplaneMessage&);
  
  virtual void add_chain(boost::shared_ptr<OperatorChain> c) {chain = c;}
  virtual ~TimerSource() { stop(); }
  
 protected:
  TimerSource(): running(false),send_now(false),exit_at_end(true),ADAPT(true){}
  
  virtual int emit_data() = 0; //returns delay (in ms) for next send. negative means to stop sending.
  
  void emit_wrapper();


  boost::shared_ptr<CongestionPolicy> congest_policy;
  volatile bool running;
  volatile bool send_now, exit_at_end;
  boost::shared_ptr<OperatorChain> chain;
  boost::shared_ptr<boost::asio::strand> st;
  
  bool ADAPT;
  boost::shared_ptr<boost::asio::deadline_timer> timer;
};

class CFileRead: public TimerSource {
 public:

  CFileRead():lineno(0) {}
  virtual operator_err_t configure(std::map<std::string,std::string> &config);
  virtual int emit_data();

  virtual std::string long_description();

 protected:
  std::string f_name; //name of file to read
  bool skip_empty; // option: skip empty lines
  std::ifstream in_file;
  unsigned lineno;

GENERIC_CLNAME
};

class CDummyReceiver: public COperator {
 public:
  std::vector< boost::shared_ptr<Tuple> > tuples;
  bool store;

  virtual void process(OperatorChain * chain,
                       std::vector<boost::shared_ptr<Tuple> > &,
                       DataplaneMessage&);
 
  virtual operator_err_t configure (std::map<std::string, std::string> & config){
    if (config["no_store"].length() > 0)
      store=false;
    return C_NO_ERR;
  }

//  virtual void process_delta (OperatorChain * chain, Tuple& oldV, boost::shared_ptr<Tuple> newV, const operator_id_t pred);
  
  virtual std::string long_description() {
      std::ostringstream buf;
      buf << tuples.size() << " stored tuples.";
      return buf.str();
  }
  
  virtual void no_more_tuples() {} //don't exit at end; keep data available
  
  CDummyReceiver(): store(true) {}

GENERIC_CLNAME
};


/**
 * Adds constant data to a tuple.
 *   Values should be named "0"..."9".
 *    If you need to add more than ten values, use two ExtendOperators!
 * Values should be parallel to a field, named types, with same syntax as
 * for the GenericParse operator.
 *  The value ${HOSTNAME} is special; it will be replaced with the host name at 
 * configuration time. 
 
*/
class CExtendOperator: public COperator {
 public:
  std::vector< Element > new_data;

  void mutate_tuple(Tuple& t);

  virtual void process(OperatorChain * chain,
                       std::vector<boost::shared_ptr<Tuple> > & tuples,
                       DataplaneMessage& m) {
    for (int i = 0; i < tuples.size(); ++i)
      mutate_tuple(*(tuples[i]));
  }
  
//  virtual void process_delta (Tuple& oldV, boost::shared_ptr<Tuple> newV, const operator_id_t pred);
  
  virtual operator_err_t configure (std::map<std::string,std::string> &config);
  
GENERIC_CLNAME
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
