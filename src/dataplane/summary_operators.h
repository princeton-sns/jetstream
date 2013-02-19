#ifndef JetStream_quantile_operators_h
#define JetStream_quantile_operators_h


#include "dataplaneoperator.h"

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



}


#endif
