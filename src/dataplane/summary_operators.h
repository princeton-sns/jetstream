#ifndef JetStream_quantile_operators_h
#define JetStream_quantile_operators_h


#include "operator_chain.h"
#include "congest_policy.h"
#include <vector>


namespace jetstream {


class QuantileOperator: public CEachOperator {
 public:
  QuantileOperator(): q(0.5) {}


  virtual void process_one(boost::shared_ptr<Tuple> & t);
  virtual operator_err_t configure(std::map<std::string,std::string> &config);

 private:
  double q;
  unsigned field;

GENERIC_CLNAME
};  


class SummaryToCount: public CEachOperator {
 public:
  SummaryToCount() {}


  virtual void process_one(boost::shared_ptr<Tuple> & t);
  virtual operator_err_t configure(std::map<std::string,std::string> &config);

 private:
  unsigned field;

GENERIC_CLNAME
};  

class ToSummary: public CEachOperator {
 public:
  ToSummary() {}


  virtual void process_one(boost::shared_ptr<Tuple> & t);
  virtual operator_err_t configure(std::map<std::string,std::string> &config);

 private:
  unsigned field;
  unsigned s_size;

GENERIC_CLNAME
};  


class DegradeSummary: public CEachOperator {
 public:
  DegradeSummary():cur_step(10) {}

  virtual void set_congestion_policy(boost::shared_ptr<CongestionPolicy> p) {
    congest_policy = p;
  }
  virtual void process_one(boost::shared_ptr<Tuple> & t);
  virtual operator_err_t configure(std::map<std::string,std::string> &config);
  virtual void start();
  
 private:
  unsigned field;
  unsigned cur_step;
  std::vector<double> steps;
  boost::shared_ptr<CongestionPolicy> congest_policy;

GENERIC_CLNAME
};  


}


#endif
