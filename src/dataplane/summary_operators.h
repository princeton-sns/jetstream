#ifndef JetStream_quantile_operators_h
#define JetStream_quantile_operators_h


#include "dataplaneoperator.h"
#include <vector>

// #include <boost/thread/thread.hpp>


namespace jetstream {



class QuantileOperator: public DataPlaneOperator {
 public:
  QuantileOperator(): q(0.5) {}


  virtual void process(boost::shared_ptr<Tuple> t);
  virtual operator_err_t configure(std::map<std::string,std::string> &config);

 private:
  double q;
  unsigned field;

GENERIC_CLNAME
};  


class SummaryToCount: public DataPlaneOperator {
 public:
  SummaryToCount() {}


  virtual void process(boost::shared_ptr<Tuple> t);
  virtual operator_err_t configure(std::map<std::string,std::string> &config);

 private:
  unsigned field;

GENERIC_CLNAME
};  

class ToSummary: public DataPlaneOperator {
 public:
  ToSummary() {}


  virtual void process(boost::shared_ptr<Tuple> t);
  virtual operator_err_t configure(std::map<std::string,std::string> &config);

 private:
  unsigned field;
  unsigned s_size;

GENERIC_CLNAME
};  


class DegradeSummary: public DataPlaneOperator {
 public:
  DegradeSummary():cur_step(10) {}

  virtual void set_congestion_policy(boost::shared_ptr<CongestionPolicy> p) {
    congest_policy = p;
  }
  virtual void process(boost::shared_ptr<Tuple> t);
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
