#ifndef JetStream_topk_source_h
#define JetStream_topk_source_h

#include "dataplaneoperator.h"
#include <string>
#include <iostream>
// #include <boost/thread/thread.hpp>
#include <boost/thread.hpp>
#include <boost/date_time/posix_time/posix_time.hpp>

namespace jetstream {

class RandSourceOperator: public DataPlaneOperator {
 private:
  double slice_min, slice_max;
  std::string drawFromDistrib(int x);

  boost::shared_ptr<boost::thread> loopThread;
  volatile bool running;


 public:
  virtual operator_err_t configure(std::map<std::string,std::string> &config);

  virtual void start();
  virtual void stop();
  void operator()();  // A thread that will loop while reading the file


GENERIC_CLNAME
};  


}

#endif
