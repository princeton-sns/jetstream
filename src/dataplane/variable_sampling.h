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
  
    //needs to respond to congestion signals
    virtual void meta_from_downstream(DataplaneMessage & msg);


    boost::shared_ptr<CongestionMonitor> congestion_monitor() {
      return boost::shared_ptr<CongestionMonitor>(new UncongestedMonitor);
    }
  
GENERIC_CLNAME

};


class CongestionController: public DataPlaneOperator {

private:
    std::map<operator_id_t,double> reportedLevels;

    std::map<operator_id_t, boost::shared_ptr<TupleSender> > predecessors;
  
  
    double targetSampleRate, worstCongestion; //should always be lower than min(reportedLevels)
    mutable boost::mutex lock;
    boost::shared_ptr<boost::asio::deadline_timer> timer;
  
  
  
public:

  CongestionController() : targetSampleRate(1.0),worstCongestion(INFINITY) {}

  virtual void process(boost::shared_ptr<Tuple> t) {
    emit(t);
  }

  virtual void meta_from_upstream(DataplaneMessage & msg, const operator_id_t pred);

  void assess_status();

GENERIC_CLNAME
};

}

#endif /* defined(__JetStream__variable_sampling__) */
