
#ifndef JetStream_ThreadedOperator_h
#define JetStream_ThreadedOperator_h


#include "dataplaneoperator.h"

#include <boost/thread.hpp>

namespace jetstream {

class ThreadedSource: public DataPlaneOperator {
 public:
//  virtual operator_err_t configure(std::map<std::string,std::string> &config);
  virtual void start();
  virtual void stop();
  virtual void process(boost::shared_ptr<Tuple> t);
  void operator()();  // A thread that will loop while reading the file
  
 protected:
  ThreadedSource(): running(false),send_now(false),exit_at_end(true) {}
  
  virtual bool emit_1() = 0; //returns true to stop sending; else false

  boost::shared_ptr<boost::thread> loopThread;
  volatile bool running;
  volatile bool send_now, exit_at_end;
  

};

}

#endif
