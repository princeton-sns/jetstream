#ifndef __JetStream__variable_sampling__
#define __JetStream__variable_sampling__

#include "base_operators.h"
#include <map>

namespace jetstream {

class VariableSamplingOperator: public SampleOperator {
  private:
    boost::shared_ptr<boost::asio::deadline_timer> timer;
    double last_reported_congestion;
    static const int REPORT_INTERVAL = 100; //ms
  
    void report_congestion();
//    boost::shared_ptr<CongestionMonitor> downstream_congestion;

  public:
  
    VariableSamplingOperator(): last_reported_congestion(INFINITY) {}
  
    virtual void start();
  
    virtual void stop() {
      if(timer)
        timer->cancel();
    }

  
    //needs to respond to congestion signals
    virtual void meta_from_downstream(const DataplaneMessage & msg);


    boost::shared_ptr<CongestionMonitor> congestion_monitor() {
      return boost::shared_ptr<CongestionMonitor>(new UncongestedMonitor);
    }
  
GENERIC_CLNAME

};


class CongestionController: public DataPlaneOperator {

private:
     int INTERVAL; //ms


    std::map<operator_id_t,double> reportedLevels;

    std::vector<boost::shared_ptr<TupleSender> > predecessors;
  
  
    double targetSampleRate, worstCongestion; //should always be lower than min(reportedLevels)
    mutable boost::mutex lock;
    boost::shared_ptr<boost::asio::deadline_timer> timer;
  
    void assess_status(); //assess and reset timer
  
  
public:

  CongestionController() : INTERVAL(300),targetSampleRate(1.0),worstCongestion(INFINITY) {}

  virtual operator_err_t configure(std::map<std::string,std::string> &config);

  virtual void stop() {
      if(timer)
        timer->cancel();
    }

  virtual void add_pred (boost::shared_ptr<TupleSender> d) { predecessors.push_back(d); }
  virtual void clear_preds () { predecessors.clear(); }

  virtual void process(boost::shared_ptr<Tuple> t) {
    emit(t);
  }

  virtual void meta_from_upstream(const DataplaneMessage & msg, const operator_id_t pred);

  void do_assess(); //externally callable, for testing

GENERIC_CLNAME
};

}

#endif /* defined(__JetStream__variable_sampling__) */
