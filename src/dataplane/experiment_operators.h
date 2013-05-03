#ifndef JetStream_experiment_operators_h
#define JetStream_experiment_operators_h

#include <string>
#include <iostream>
#include <queue>

#include <boost/thread.hpp>
#include <boost/date_time/posix_time/posix_time.hpp>

#include "dataplaneoperator.h"
#include "chain_ops.h"
#include "queue_congestion_mon.h"

// #include <boost/thread/thread.hpp>


namespace jetstream {

class DummyReceiver: public COperator {
 public:
  std::vector< boost::shared_ptr<Tuple> > tuples;
  bool store;

  virtual void process(OperatorChain * chain,
                       std::vector<boost::shared_ptr<Tuple> > &,
                       DataplaneMessage&);
 
  virtual operator_err_t configure (std::map<std::string, std::string> & config){
    if (config["no_store"].length() > 0)
      store=false;
    return NO_ERR;
  }

//  virtual void process_delta (OperatorChain * chain, Tuple& oldV, boost::shared_ptr<Tuple> newV, const operator_id_t pred);
  
  virtual std::string long_description() {
      std::ostringstream buf;
      buf << tuples.size() << " stored tuples.";
      return buf.str();
  }
  
  virtual void no_more_tuples() {} //don't exit at end; keep data available
  
  DummyReceiver(): store(true) {}

GENERIC_CLNAME
};


class xDummyReceiver: public DataPlaneOperator {
 public:
  std::vector< boost::shared_ptr<Tuple> > tuples;
  bool store;

  virtual void process(boost::shared_ptr<Tuple> t) {
    if(store)
      tuples.push_back(t);
  }
 
  virtual operator_err_t configure (std::map<std::string, std::string> & config){
    if (config["no_store"].length() > 0)
      store=false;
    return NO_ERR;
  }


  virtual void process_delta (Tuple& oldV, boost::shared_ptr<Tuple> newV, const operator_id_t pred) {}

  
  virtual std::string long_description() {
      std::ostringstream buf;
      buf << tuples.size() << " stored tuples.";
      return buf.str();
  }
  
  virtual void no_more_tuples() {} //don't exit at end; keep data available
  
  virtual ~xDummyReceiver() {}
  xDummyReceiver(): store(true) {}
  

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
 * Operator for periodically emitting a specified number of generic tuples.
 */
class ContinuousSendK: public TimerSource {
 public:
  ContinuousSendK():num_sent(0) {}
  virtual operator_err_t configure(std::map<std::string,std::string> &config);


 protected:
  virtual int emit_data() ;
  int32_t num_sent;
  u_long k;       // Number of tuples to send
  msec_t period;  // Time to wait before sending next k tuples  

GENERIC_CLNAME
};  


class SerDeOverhead: public CEachOperator {
 public:
  virtual void process_one(boost::shared_ptr<Tuple>& t);
  virtual operator_err_t configure(std::map<std::string,std::string> &config) {
    return NO_ERR;
  }

GENERIC_CLNAME
};  


class EchoOperator: public CEachOperator {
 public:
  EchoOperator(): o(&std::cout) {}


  virtual void process_one(boost::shared_ptr<Tuple>& t);
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
class FixedRateQueue: public TimerSource {
 public:

  FixedRateQueue() //,elements_queued(0)
  {}
  
  virtual ~FixedRateQueue() {
  }
  
  virtual operator_err_t configure(std::map<std::string,std::string> &config);
  virtual void process(OperatorChain * chain, std::vector<boost::shared_ptr<Tuple> > &, DataplaneMessage&);

  virtual bool is_chain_end() {return true;}

  virtual int emit_data();
  
  virtual boost::shared_ptr<CongestionMonitor> congestion_monitor() {
    return mon;
  }
  
  int queue_length() {
//    return mon->queue_length();
    return q.size();
  }
  
private:
  int ms_per_dequeue;
//  int elements_queued;
  std::queue< DataplaneMessage > q;
  boost::mutex mutex;
  boost::shared_ptr<NetCongestionMonitor> mon;


GENERIC_CLNAME
};  



class ExperimentTimeRewrite: public CEachOperator {
 public:

  ExperimentTimeRewrite(): warp(0),first_tuple_t(0),delta(0),field(0),emitted(0) {}
  virtual void process_one(boost::shared_ptr<Tuple>& t);
  virtual operator_err_t configure(std::map<std::string,std::string> &config);

 private:
  double warp;
  time_t first_tuple_t; //the tuple time of first tuple
  time_t delta; //offset from simulation to reality for first tuple
//  unsigned t_count;
  unsigned field;
  unsigned emitted;
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

  AvgCongestLogger(): report_interval(2000),last_bytes(0),tuples_in_interval(0), field(-1),
      count_tally(0), hist_field(-1), hist_size_total(0), err_bound(0) {}
  virtual void process(boost::shared_ptr<Tuple> t); 
  virtual operator_err_t configure(std::map<std::string,std::string> &config);
  virtual void meta_from_upstream(const DataplaneMessage & msg, const operator_id_t pred);
  virtual void start();
  virtual void stop();
  void report();

  virtual void no_more_tuples() {} //don't exit at end, since we take multiple inputs


 private:
  std::map<operator_id_t, unsigned> window_for;
  std::map<operator_id_t, double> sample_lev_for;

  boost::mutex mutex;
  volatile bool running;
  boost::shared_ptr<boost::asio::deadline_timer> timer;
  
  unsigned report_interval;
  uint64_t last_bytes;
  unsigned tuples_in_interval;
  int field;
  uint64_t count_tally;
  int hist_field;
  uint64_t hist_size_total;
  unsigned err_bound;
GENERIC_CLNAME
};  


  
}



#endif
