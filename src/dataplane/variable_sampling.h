#ifndef __JetStream__variable_sampling__
#define __JetStream__variable_sampling__

#include "base_operators.h"
#include <map>

namespace jetstream {


class PeriodicCongestionReporter {

private:
    static const int REPORT_INTERVAL = 100; //ms

    double last_reported_congestion;
    DataPlaneOperator * parent;
    boost::shared_ptr<boost::asio::deadline_timer> timer;
    volatile bool running;

    boost::shared_ptr<TupleReceiver> dest;
  
    void report_congestion();
    mutable boost::recursive_mutex lock;
  

public:

  PeriodicCongestionReporter (DataPlaneOperator * p):
        last_reported_congestion(INFINITY), parent(p), running(true) {
  } ;
  
  void stop() { //should be idempotent
    boost::lock_guard<boost::recursive_mutex> critical_section (lock);
    running = false;
    dest.reset();
    if(timer)
      timer->cancel();
  }
  
  void set_dest(boost::shared_ptr<TupleReceiver> d) {
    dest = d;
  }
  
  void start(boost::shared_ptr<boost::asio::deadline_timer> t);

  ~PeriodicCongestionReporter() {
    stop();
  }


};

class VariableSamplingOperator: public HashSampleOperator {
  private:
//    PeriodicCongestionReporter reporter;
//    boost::shared_ptr<CongestionMonitor> downstream_congestion;

  public:
  
    VariableSamplingOperator(): num_steps(11){}
  
  
    virtual operator_err_t configure(std::map<std::string,std::string> &config);
    
    virtual void start();
  
    virtual void stop() {
//      reporter.stop();
    }
  
    //needs to respond to congestion signals
//    virtual void meta_from_downstream(const DataplaneMessage & msg);
    virtual void meta_from_upstream(const DataplaneMessage & msg, const operator_id_t pred);

    virtual void set_congestion_policy(boost::shared_ptr<CongestionPolicy> p) {
      congest_policy = p;
    }

 private:
  boost::mutex mutex; 
  unsigned cur_step;
  std::vector<double> steps;
  unsigned num_steps;
  boost::shared_ptr<CongestionPolicy> congest_policy;



GENERIC_CLNAME

};


class CongestionController: public DataPlaneOperator {

private:
     int INTERVAL; //ms
     static const int REPORT_TIMEOUT = 4; //seconds


    std::map<operator_id_t,double> reportedLevels;
    std::map<operator_id_t,time_t> timeOfReport;


    std::vector< boost::shared_ptr<TupleSender> > predecessors;
  
  
    double targetSampleRate, worstCongestion; //should always be lower than min(reportedLevels)
    mutable boost::recursive_mutex lock;
    boost::shared_ptr<boost::asio::deadline_timer> timer;
  
    void assess_status(); //assess and reset timer
  
  
public:

  CongestionController() : INTERVAL(300),targetSampleRate(1.0),worstCongestion(INFINITY) {}

  virtual operator_err_t configure(std::map<std::string,std::string> &config);

  virtual void start() {
    do_assess();
  }

  virtual void stop() {
    boost::lock_guard<boost::recursive_mutex> critical_section (lock);

    boost::shared_ptr<boost::asio::deadline_timer> t2 = timer;
    timer.reset(); //so that assess notices it's being cancelled
    if (t2)
      t2->cancel();
  }

  virtual void add_pred (boost::shared_ptr<TupleSender> d) { predecessors.push_back(d); }
  virtual void clear_preds () { predecessors.clear(); }
  virtual void remove_pred (operator_id_t);


  virtual void process(boost::shared_ptr<Tuple> t) {
    emit(t);
  }

  virtual void meta_from_upstream(const DataplaneMessage & msg, const operator_id_t pred);

  void do_assess(); //externally callable, for testing

  virtual std::string long_description();
  
  virtual ~CongestionController(); 


GENERIC_CLNAME
};

}

#endif /* defined(__JetStream__variable_sampling__) */
