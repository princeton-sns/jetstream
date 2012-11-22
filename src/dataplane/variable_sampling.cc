
#include "variable_sampling.h"
#include <glog/logging.h>

using namespace ::std;
using namespace boost;

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
VariableSamplingOperator::meta_from_downstream(DataplaneMessage & msg) {
  LOG(INFO) << id() << " got " << msg.Utf8DebugString();
  
  if ( msg.type() == DataplaneMessage::SET_BACKOFF) {
    
  
  }
}

void
CongestionController::meta_from_upstream(DataplaneMessage & msg, const operator_id_t pred) {

  if (msg.type() == DataplaneMessage::CONGEST_STATUS) { //congestion report
    lock_guard<boost::mutex> critical_section (lock);

    double lev = msg.congestion_level();
    reportedLevels[pred] = lev;
    
    if (lev < worstCongestion) {
      worstCongestion = lev;
      if (!timer) {
        timer = get_timer();
        timer->expires_from_now(boost::posix_time::millisec(500));
        timer->async_wait(boost::bind(&CongestionController::assess_status, this));
      }
    }
    
  }

}


void
CongestionController::assess_status() {
  
  //foreach pred: send a change-of-status
  
  timer->expires_from_now(boost::posix_time::millisec(500));
  timer->async_wait(boost::bind(&CongestionController::assess_status, this));
}


const string VariableSamplingOperator::my_type_name("Variable sampling operator");
const string CongestionController::my_type_name("Congestion controller");

}