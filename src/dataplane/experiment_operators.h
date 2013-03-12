#ifndef JetStream_experiment_operators_h
#define JetStream_experiment_operators_h

#include <string>
#include <iostream>
#include <queue>

#include <boost/thread.hpp>
#include <boost/date_time/posix_time/posix_time.hpp>

#include "dataplaneoperator.h"
#include "threaded_source.h"
#include "queue_congestion_mon.h"

// #include <boost/thread/thread.hpp>


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
class ContinuousSendK: public ThreadedSource {
 public:
  ContinuousSendK():num_sent(0) {}
  virtual operator_err_t configure(std::map<std::string,std::string> &config);


 protected:
  virtual bool emit_1() ;
  boost::shared_ptr<Tuple> t;
  int32_t num_sent;
  u_long k;       // Number of tuples to send
  msec_t period;  // Time to wait before sending next k tuples  

GENERIC_CLNAME
};  


class SerDeOverhead: public DataPlaneOperator {
 public:
  virtual void process(boost::shared_ptr<Tuple> t);

GENERIC_CLNAME
};  


class EchoOperator: public DataPlaneOperator {
 public:
  EchoOperator(): o(&std::cout) {}


  virtual void process(boost::shared_ptr<Tuple> t);
  virtual operator_err_t configure(std::map<std::string,std::string> &config);
  virtual ~EchoOperator();

 private:
  std::ostream* o;


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


/***
 * Operator for artificially imposing congestion.
 */
class FixedRateQueue: public DataPlaneOperator {
 public:

  FixedRateQueue() : running(false) //,elements_queued(0)
  {}
  
  virtual ~FixedRateQueue() {
    stop();
  }
  
  virtual operator_err_t configure(std::map<std::string,std::string> &config);
  virtual void start();
  virtual void process(boost::shared_ptr<Tuple> t);
  virtual void stop();

  void process1();
  
  virtual boost::shared_ptr<CongestionMonitor> congestion_monitor() {
    return mon;
  }
  
  int queue_length() {
//    return mon->queue_length();
    return q.size();
  }
  
    virtual void meta_from_upstream(const DataplaneMessage & msg, const operator_id_t pred);

  
private:
  volatile bool running;
  int ms_per_dequeue;
//  int elements_queued;
  boost::shared_ptr<boost::asio::deadline_timer> timer;
  std::queue< DataplaneMessage > q;
  boost::mutex mutex;
  boost::shared_ptr<NetCongestionMonitor> mon;


GENERIC_CLNAME
};  



class ExperimentTimeRewrite: public DataPlaneOperator {
 public:

  ExperimentTimeRewrite(): warp(0),first_tuple_t(0),delta(0),field(0) {}
  virtual void process(boost::shared_ptr<Tuple> t);
  virtual operator_err_t configure(std::map<std::string,std::string> &config);

 private:
  double warp;
  time_t first_tuple_t; //the tuple time of first tuple
  time_t delta; //offset from simulation to reality for first tuple
//  unsigned t_count;
  unsigned field;
  bool wait_for_catch_up;


GENERIC_CLNAME
};  


class CountLogger: public DataPlaneOperator {
  //logs the total counts going past
 public:

  CountLogger(): tally_in_window(0),field(0) {}
  virtual void process(boost::shared_ptr<Tuple> t);
  virtual operator_err_t configure(std::map<std::string,std::string> &config);
  virtual void meta_from_upstream(const DataplaneMessage & msg, const operator_id_t pred);

 private:
  unsigned tally_in_window;
  unsigned field;


GENERIC_CLNAME
};  


class AvgCongestLogger: public DataPlaneOperator {
  //logs the total counts going past
 public:

  AvgCongestLogger(): report_interval(2000),last_bytes(0),tuples_in_interval(0)  {}
  virtual void process(boost::shared_ptr<Tuple> t); 
//  virtual operator_err_t configure(std::map<std::string,std::string> &config);
  virtual void meta_from_upstream(const DataplaneMessage & msg, const operator_id_t pred);
  virtual void start();
  virtual void stop();
  void report();


 private:
  std::map<operator_id_t, unsigned> window_for;
  boost::mutex mutex;
  volatile bool running;
  boost::shared_ptr<boost::asio::deadline_timer> timer;
  unsigned report_interval;
  uint64_t last_bytes;
  unsigned tuples_in_interval;

GENERIC_CLNAME
};  


  
}



#endif
