#ifndef JetStream_experiment_operators_h
#define JetStream_experiment_operators_h

#include "dataplaneoperator.h"
#include "threaded_source.h"

#include <string>
#include <iostream>
// #include <boost/thread/thread.hpp>
#include <boost/thread.hpp>
#include <boost/date_time/posix_time/posix_time.hpp>


namespace jetstream {


class DummyReceiver: public DataPlaneOperator {
 public:
  std::vector< boost::shared_ptr<Tuple> > tuples;
  virtual void process(boost::shared_ptr<Tuple> t) {
    tuples.push_back(t);
  }
  
  virtual std::string long_description() {
      std::ostringstream buf;
      buf << tuples.size() << " stored tuples.";
      return buf.str();
  }
  
  virtual void no_more_tuples() {} //don't exit at end; keep data available
  
  virtual ~DummyReceiver() {}


GENERIC_CLNAME
};


class RateRecordReceiver: public DataPlaneOperator {

 protected:
  volatile bool running;

  boost::mutex mutex;
  
  boost::posix_time::ptime window_start;
  
  volatile long tuples_in_window;
  volatile long bytes_in_window;

  double bytes_per_sec;
  double tuples_per_sec;

  boost::shared_ptr<boost::thread> loopThread;


 public:
   RateRecordReceiver():
     running(false), tuples_in_window(0),bytes_in_window(0) {}
 
  virtual void process(boost::shared_ptr<Tuple> t);
  
  virtual std::string long_description();
  
  virtual void no_more_tuples() {} //don't exit at end; keep data available
  
  virtual void start();
  virtual void stop();
  void operator()();  // A thread that will loop while reading the file    


GENERIC_CLNAME
};



/***
 * Operator for emitting a specified number of generic tuples.
 */
class SendK: public ThreadedSource {
 public:
  virtual operator_err_t configure(std::map<std::string,std::string> &config);
  void reset() { n = 1; }

 protected:
  virtual bool emit_1();
  boost::shared_ptr<Tuple> t;
  u_int64_t k, n;  // Number of tuples to send and number sent, respectively
  


GENERIC_CLNAME
};  


/***
 * Operator for periodically emitting a specified number of generic tuples.
 */
class ContinuousSendK: public DataPlaneOperator {
 public:
  virtual operator_err_t configure(std::map<std::string,std::string> &config);


 protected:
  virtual bool emit_1() ;
  boost::shared_ptr<Tuple> t;
  u_long k;       // Number of tuples to send
  msec_t period;  // Time to wait before sending next k tuples
  boost::shared_ptr<boost::thread> loopThread;
  volatile bool running;
  volatile bool send_now;
  

GENERIC_CLNAME
};  


class SerDeOverhead: public DataPlaneOperator {
 public:
  virtual void process(boost::shared_ptr<Tuple> t);


GENERIC_CLNAME
};  


class EchoOperator: public DataPlaneOperator {
 public:
  virtual void process(boost::shared_ptr<Tuple> t);


GENERIC_CLNAME
};  


/***
 * Operator for artificially imposing congestion.
 */
class MockCongestion: public DataPlaneOperator {
 public:
  virtual void process(boost::shared_ptr<Tuple> t) {
    emit(t);
  }
  virtual boost::shared_ptr<CongestionMonitor> congestion_monitor() {
    return mon;
  }

  MockCongestion();

  double congestion;

private:
  boost::shared_ptr<CongestionMonitor> mon;

GENERIC_CLNAME
};  

  
}



#endif
