
#include "variable_sampling.h"
#include <glog/logging.h>

#include <boost/interprocess/detail/atomic.hpp>
#include "net_congest_mon.h"

using namespace ::std;
using namespace boost;
using namespace boost::interprocess::ipcdetail;

namespace jetstream {


/*
void
PeriodicCongestionReporter::start(boost::shared_ptr<boost::asio::deadline_timer> t) {  
  timer = t;
  timer->expires_from_now(boost::posix_time::millisec(REPORT_INTERVAL));
  timer->async_wait(boost::bind(&PeriodicCongestionReporter::report_congestion, this));
}


void
PeriodicCongestionReporter::report_congestion() {
  if (running) {

    lock_guard<boost::recursive_mutex> critical_section (lock);
    shared_ptr<CongestionMonitor> downstream_congestion = dest->congestion_monitor();
    double congestion = downstream_congestion->capacity_ratio();


//    LOG(INFO) << id() << "checking congestion; got " << congestion;
    //can bail on reporting here if is redundant?
    
    DataplaneMessage levelMsg;
    levelMsg.set_congestion_level(congestion);
    levelMsg.set_type(DataplaneMessage::CONGEST_STATUS);
    dest->meta_from_upstream( levelMsg, parent->id());

    timer->expires_from_now(boost::posix_time::millisec(REPORT_INTERVAL));
    timer->async_wait(boost::bind(&PeriodicCongestionReporter::report_congestion, this));
  }
  
}
*/

operator_err_t
VariableSamplingOperator::configure(std::map<std::string,std::string> &config) {
  if((config["steps"].length() > 0) && !(stringstream(config["steps"]) >> num_steps)) {
    return operator_err_t("steps must be an int");
  }
  LOG(INFO) << "Configured VariableSamplingOperator. num_steps="<<num_steps;
  return HashSampleOperator::configure(config);
}


void
VariableSamplingOperator::start() {
    for( unsigned i =0; i < num_steps ; ++i) {
      steps.push_back( double(i) / (num_steps - 1) );
    }
    cur_step = num_steps-1;
//  reporter.set_dest(get_dest());
//  reporter.start(get_timer());
    cur_step += congest_policy->get_step(id(), steps.data(), steps.size(), cur_step);
    unsigned thresh = (1 - steps[cur_step]) * numeric_limits<uint32_t>::max();
    VLOG(1) << " Initial step for " << id() << " is " << cur_step << " thresh is " << thresh;
    atomic_write32(&threshold, thresh); //start sending everything
}

/*

void
VariableSamplingOperator::meta_from_downstream(const DataplaneMessage & msg) {
  
  if ( msg.type() == DataplaneMessage::SET_BACKOFF) {
    double frac_to_drop = 1.0 - msg.filter_level();
    VLOG(1) << id() << " setting filter level to " << msg.filter_level();

    assert( frac_to_drop <= 1);
    uint32_t new_thresh = frac_to_drop * numeric_limits<uint32_t>::max();
    atomic_write32(&threshold, new_thresh);
  } else {
    DataPlaneOperator::meta_from_downstream(msg);
  }
}

*/


void
VariableSamplingOperator::end_of_window( DataplaneMessage & msg){
//    boost::lock_guard<boost::mutex> lock (mutex);
  int delta = congest_policy->get_step(id(), steps.data(), steps.size(), cur_step);
  if (delta != 0) {
//    congest_policy->set_effect_delay(id(), msg.window_length_ms() * 2);
    cur_step += delta;
    uint32_t thresh = (1 - steps[cur_step]) * numeric_limits<uint32_t>::max();
    atomic_write32(&threshold, thresh);
    LOG(INFO) << "hash-filtering will let through " <<
       100 * steps[cur_step] << "% of data";
  }
  msg.set_filter_level(  steps[cur_step]);
  
}



operator_err_t
IntervalSamplingOperator::configure(std::map<std::string,std::string> &config) {
  if((config["max_drops"].length() > 0) && !(stringstream(config["max_drops"]) >> max_drops)) {
    return operator_err_t("max_drops must be an int");
  }
  
  for( int drops_per =max_drops; drops_per >= 0; --drops_per) {
    steps.push_back(  1.0 / (1+drops_per) );
  }
  
  LOG(INFO) << "Configured " << my_type_name <<  ". max_drops="<<max_drops;
  return NO_ERR;
}

void
IntervalSamplingOperator::start() {

}



bool
IntervalSamplingOperator::should_emit(const jetstream::Tuple &t) {

  int cur_step = steps.size() - drops_per_keep -1;
  int delta = congest_policy->get_step(id(), steps.data(), steps.size(), cur_step);
  if (delta != 0) {
    NetCongestionMonitor * mon = dynamic_cast<NetCongestionMonitor*>(congest_policy->get_monitor());
    if (mon) {
      LOG(INFO) << "Switching drop frequency by " << delta << " to " <<
          (drops_per_keep - delta)<< ". Congest mon state: " << mon->long_description();
    }
  }
  drops_per_keep -= delta; 

  cntr++;
  //cntr %= drops_per_keep +1;
  return cntr % (drops_per_keep + 1) == 0;
}

/*

void
CongestionController::remove_pred (operator_id_t d) {
  vector<  boost::shared_ptr<TupleSender> >::iterator it = predecessors.begin();
  while ( it != predecessors.end()) {
     boost::shared_ptr<DataPlaneOperator> p = boost::dynamic_pointer_cast<DataPlaneOperator>(*it);
     if (p && p->id() == d)
      break;
      else
      it++;
  }
  if( it != predecessors.end())
    predecessors.erase(it);
//  LOG(FATAL) << "unimplemented";
  
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
    lock_guard<boost::recursive_mutex> critical_section (lock);

    double lev = msg.congestion_level();
    reportedLevels[pred] = lev;
    timeOfReport[pred] = time(NULL);

//    LOG(INFO) << "Congestion at " << pred << " - " << lev;

    
    if (lev < worstCongestion) {
      worstCongestion = lev;
      if (!timer) {
        timer = get_timer();
        timer->expires_from_now(boost::posix_time::millisec(INTERVAL));
        timer->async_wait(boost::bind(&CongestionController::assess_status, this));
      }
    }
    
  } else {
    DataPlaneOperator::meta_from_upstream(msg, pred);
  }

}


void
CongestionController::do_assess() {  
  worstCongestion = INFINITY;
  time_t now = time(NULL);
  map<operator_id_t, double>::iterator it = reportedLevels.begin();
  while (it != reportedLevels.end()) {
    time_t reportTime = timeOfReport[it->first];
    if (reportTime + REPORT_TIMEOUT < now) {
      operator_id_t toDel = it->first;
      it ++;
      reportedLevels.erase(toDel);
      timeOfReport.erase(toDel);
    }
    else {
      if (it->second < worstCongestion)
        worstCongestion = it->second;
      it++;
    }
  }
  
  //TODO should we clear reportedLevels ?
  

  if (targetSampleRate < 0.001 && worstCongestion != 0)
    targetSampleRate = 0.001; //never back off more than a factor of a thousand

  if ( worstCongestion  > 1.01 || worstCongestion < 0.99 ) { //not quite in balance
    targetSampleRate *= worstCongestion;
    if (targetSampleRate > 0.999)
      targetSampleRate = 1; //never send > 1

    LOG_EVERY_N(INFO, 5)<< id() << " setting filter target level to "<< targetSampleRate
    << " and sending to "  <<  predecessors.size() << " preds";


    DataplaneMessage throttle;
    throttle.set_filter_level(targetSampleRate);
    throttle.set_type(DataplaneMessage::SET_BACKOFF);

//    LOG(INFO) << id() << " has " << predecessors.size() << " preds";

    for (unsigned int i = 0; i < predecessors.size(); ++i) {
      shared_ptr<TupleSender> pred = predecessors[i];
//      LOG(INFO) << "sending backoff to pred " << i;
      pred->meta_from_downstream(throttle);
    }
  }
}

void
CongestionController::assess_status() {
  lock_guard<boost::recursive_mutex> critical_section (lock);
  if (timer) {
    do_assess();
    timer->expires_from_now(boost::posix_time::millisec(INTERVAL));
    timer->async_wait(boost::bind(&CongestionController::assess_status, this));
  }
}


CongestionController::~CongestionController() {
  stop();
}

std::string
CongestionController::long_description() {
  ostringstream o;
  o << "selectivity = " << targetSampleRate;
  return o.str();
}
*/

const string VariableSamplingOperator::my_type_name("Variable sampling operator");
const string IntervalSamplingOperator::my_type_name("Fixed-interval sampling operator");
//const string CongestionController::my_type_name("Congestion controller");

}
