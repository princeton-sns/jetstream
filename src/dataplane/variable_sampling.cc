
#include "variable_sampling.h"
#include <glog/logging.h>

#include <boost/interprocess/detail/atomic.hpp>


using namespace ::std;
using namespace boost;
using namespace boost::interprocess::ipcdetail;

namespace jetstream {

void
VariableSamplingOperator::start() {

  timer = get_timer();
  timer->expires_from_now(boost::posix_time::millisec(REPORT_INTERVAL));
  timer->async_wait(boost::bind(&VariableSamplingOperator::report_congestion, this));

//  set_timer_task(boost::bind(VariableSamplingOperator::report_status, this ), 500);

}

void
VariableSamplingOperator::report_congestion() {

  shared_ptr<TupleReceiver> dest = get_dest ();
  if (dest) {
    shared_ptr<CongestionMonitor> downstream_congestion = dest->congestion_monitor();
    double congestion = downstream_congestion->capacity_ratio();


//    LOG(INFO) << id() << "checking congestion; got " << congestion;
    //can bail on reporting here if is redundant?
    
    DataplaneMessage levelMsg;
    levelMsg.set_congestion_level(congestion);
    levelMsg.set_type(DataplaneMessage::CONGEST_STATUS);
    dest->meta_from_upstream( levelMsg, id());
  }
  timer->expires_from_now(boost::posix_time::millisec(REPORT_INTERVAL));
  timer->async_wait(boost::bind(&VariableSamplingOperator::report_congestion, this));
  
}

void
VariableSamplingOperator::meta_from_downstream(const DataplaneMessage & msg) {
  LOG(INFO) << id() << " got " << msg.Utf8DebugString();
  
  if ( msg.type() == DataplaneMessage::SET_BACKOFF) {
    double frac_to_drop = 1.0 - msg.filter_level();
    assert( frac_to_drop <= 1);
    uint32_t new_thresh = frac_to_drop * numeric_limits<uint32_t>::max();
    atomic_write32(&threshold, new_thresh);
  }
}


operator_err_t
CongestionController::configure(std::map<std::string,std::string> &config) {

  if (config["interval"].length() > 0) {
    if (!(stringstream(config["interval"]) >> INTERVAL)) {
      string msg = "invalid reconfigure interval ('interval'): " + config["INTERVAL"];
      return operator_err_t(msg);
    }
  }
  return NO_ERR;
}


void
CongestionController::meta_from_upstream(const DataplaneMessage & msg, const operator_id_t pred) {

  if (msg.type() == DataplaneMessage::CONGEST_STATUS) { //congestion report
    lock_guard<boost::mutex> critical_section (lock);

    double lev = msg.congestion_level();
    reportedLevels[pred] = lev;
    
    if (lev < worstCongestion) {
      worstCongestion = lev;
      if (!timer) {
        timer = get_timer();
        timer->expires_from_now(boost::posix_time::millisec(INTERVAL));
        timer->async_wait(boost::bind(&CongestionController::assess_status, this));
      }
    }
    
  }

}


void
CongestionController::do_assess() {
  lock_guard<boost::mutex> critical_section (lock);
  
  worstCongestion = INFINITY;
  
  map<operator_id_t, double>::iterator it = reportedLevels.begin();
  while (it != reportedLevels.end()) {
    if (it->second < worstCongestion)
      worstCongestion = it->second;
    it++;
  }
  
  //TODO should we clear reportedLevels ?
  

  if ( worstCongestion  > 1.01 || worstCongestion < 0.99 ) { //not quite in balance
    targetSampleRate *= worstCongestion;
    LOG(INFO)<< id() << " setting filter target level to "<< targetSampleRate;
    if (targetSampleRate > 0.999)
      targetSampleRate = 1; //never send > 1
      //foreach pred: send a change-of-status

    DataplaneMessage throttle;
    throttle.set_filter_level(targetSampleRate);
    throttle.set_type(DataplaneMessage::SET_BACKOFF);

//    LOG(INFO) << id() << " has " << predecessors.size() << " preds";

    for (int i = 0; i < predecessors.size(); ++i) {
      shared_ptr<TupleSender> pred = predecessors[i];
//      LOG(INFO) << "sending backoff to pred " << i;
      pred->meta_from_downstream(throttle);
    }
  }
}

void
CongestionController::assess_status() {
  do_assess();
  timer->expires_from_now(boost::posix_time::millisec(INTERVAL));
  timer->async_wait(boost::bind(&CongestionController::assess_status, this));
}


const string VariableSamplingOperator::my_type_name("Variable sampling operator");
const string CongestionController::my_type_name("Congestion controller");

}