
#include "queue_congestion_mon.h"
#include "window_congest_mon.h"
#include <glog/logging.h>
#include <algorithm> 
#include <numeric>

using namespace boost::interprocess::ipcdetail;
using namespace ::std;

namespace jetstream {

const int N_TO_LOG = 211;

double
QueueCongestionMonitor::capacity_ratio() {
  boost::unique_lock<boost::recursive_mutex> lock(internals);
  usec_t now = get_usec();

    //only sample every 100 ms
  if (now > lastQueryTS + SAMPLE_INTERVAL_MS * 1000) {
//    usec_t sample_period = now - lastQueryTS;
    lastQueryTS = now;

    uint32_t readQLen = atomic_read32(&queueLen);

    uint32_t inserts = atomic_read32(&insertsInPeriod);
    uint32_t newi  = inserts;
    while ( ( newi = atomic_cas32(&insertsInPeriod, 0, newi)) != 0) { //reset to zero atomically with read
      if(inserts != newi)
        readQLen = atomic_read32(&queueLen); //prevents readQLen from being too low => causes prevQueueLen to be too low on next iteration => queueDelta too high => removes negative.
      inserts = newi;
    }
    prev_inserts = inserts;

    int32_t queueDelta = readQLen - prevQueueLen; //negative implies queue is shrinking
    int32_t availRoom = queueTarget - readQLen - queueDelta; //negative 'availRoom' implies we need to cut the rate
    if (  ((int32_t)inserts ) < -availRoom) // Signed compare: queue won't drain fast enough enough room even with no inserts
      ratio = 0; //should stop sends altogether to let things drain
    else
      ratio = availRoom /(double) inserts + 1.0;

    double rate_per_sec = inserts * 1000.0 / SAMPLE_INTERVAL_MS;
    ratio = fmin(ratio, max_per_sec / rate_per_sec);
    removes = inserts - queueDelta;
    if (removes < 0) {
      LOG(WARNING) << "Shouldn't have data leaking out of " << name()    
        <<  ". Inserts: " << inserts <<" queueDelta: " <<queueDelta << " readQLen: " << readQLen << " prevQueueLen: "<<prevQueueLen;
      removes = 0;
    }

//    LOG_IF_EVERY_N(INFO, readQLen > 0 || inserts > 0 || prevRatio == 0 , N_TO_LOG) <<
 //   LOG(INFO) <<
//        "(logged every "<<N_TO_LOG<<") Queue for " << name() << ": " << long_description();
    LOG_IF(FATAL, ratio < 0) << "ratio should never be negative";
    prevQueueLen = readQLen;
  } 

  return fmin(ratio, downstream_status);
}


std::string
QueueCongestionMonitor::long_description() {
  boost::unique_lock<boost::recursive_mutex> lock(internals);

  ostringstream buf;
//  int32_t removes = insertsInPeriod + prevQueueLen - queueLen;
  
  
  double result = fmin(ratio,downstream_status);

  buf << prev_inserts << " inserts (configured max rate is " << max_per_sec << "); " << removes
      <<" removes. Final queue length " << prevQueueLen << "/" << queueTarget <<
      ". Space Ratio is " << result <<
            ", downstream is " << downstream_status<< " and final result is " << result;
  return buf.str();
}


msec_t
QueueCongestionMonitor::measurement_time() {
  if (downstream_status < ratio)
    return downstream_report_time;
  else
    return lastQueryTS;
}



WindowCongestionMonitor::WindowCongestionMonitor(const std::string& name, double smoothing_prev_value):
  NetCongestionMonitor(name), window_start_time(0),
      last_window_end(get_msec()), bytes_in_window(0), smoothing_factor(smoothing_prev_value) {
}


void
WindowCongestionMonitor::end_of_window(int window_data_ms, msec_t processing_start_time) {
  boost::unique_lock< boost::recursive_mutex> lock(internals);
  msec_t now = get_msec();

  double unsmoothed_ratio;
  if (processing_start_time != 0) {
    msec_t window_processtime_ms = now - processing_start_time;
    msec_t window_availtime_ms = now - last_window_end;
    
    double window_ratio = double(window_data_ms) / window_processtime_ms;
    double bytes_per_sec = bytes_in_window * 1000.0 / window_availtime_ms;
    unsmoothed_ratio =  std::min(window_ratio, max_per_sec / bytes_per_sec) ;
    LOG_EVERY_N(INFO, 20) << long_description() <<". Nominal window size was " << window_data_ms;

  if (unsmoothed_ratio > 10)
    unsmoothed_ratio = 10;      //limit ramp-up and avoid confusing the sliding window
  } else { //no data in window; we are therefore UNCONSTRAINED
    unsmoothed_ratio = 10;
  }
  ratio = (1 - smoothing_factor) * unsmoothed_ratio + smoothing_factor * ratio;
  bytes_in_window = 0;
  last_window_end = now;
  window_start_time = 0;
  
}

double
WindowCongestionMonitor::capacity_ratio() {
  boost::unique_lock<boost::recursive_mutex> lock(internals);    
  return fmin(downstream_status, ratio);
  //downstream status can change asynchronously. So can't roll it into last_ratio
}               

void
WindowCongestionMonitor::report_insert(void * item, uint32_t weight) {
  if (window_start_time == 0) {
//      LOG(INFO) << "SAW A SEND";
    boost::unique_lock<boost::recursive_mutex> lock(internals);
    if (window_start_time == 0) //doublecheck now that we have lock
      window_start_time = get_msec();
  }
  bytes_in_window += weight;
}

msec_t
WindowCongestionMonitor::measurement_time() {
  if (downstream_status < ratio)
    return downstream_report_time;
  else
    return last_window_end;
}

std::string
WindowCongestionMonitor::long_description() {
  boost::unique_lock<boost::recursive_mutex> lock(internals);
  ostringstream buf;

  msec_t now = get_msec();

  msec_t window_processtime_ms = now - window_start_time;
  msec_t window_availtime_ms = now - last_window_end;
  
  double bytes_per_sec = bytes_in_window * 1000.0 / window_availtime_ms;
  
  
  buf << "Capacity ratio at " << name() << " is now "
      << ratio <<  ". Durations were " << window_processtime_ms << "/"
      << window_availtime_ms  // <<". Nominal window size was " << window_data_ms
      <<", saw " << bytes_per_sec << " bytes/sec. Final ratio " <<  fmin(downstream_status, ratio);

  return buf.str();
}

static const unsigned WIND_SIZE = 2;
static const double PROJECT_STEPS = 6.0;

SmoothingQCongestionMonitor::SmoothingQCongestionMonitor(uint32_t qTarg, const std::string& nm, msec_t step_ms):
      GenericQCongestionMonitor(qTarg, nm), insertsInPeriod(0), removesInPeriod(0),
        v_idx(0), SMOOTH_STEP_MS(step_ms)  {
  for (unsigned i = 0; i < WIND_SIZE; ++i) {
    inserts.push_back(0);
    removes.push_back(0);
    tstamps.push_back(0);
  }
}

inline unsigned write_and_clear(uint32_t *targ) {
  //we have cas, but not atomic_swap
  uint32_t most_recent_val = atomic_read32(targ);
  
  uint32_t final_val  = most_recent_val;
  while ( ( most_recent_val = atomic_cas32(targ, 0, most_recent_val)) != final_val) { //reset to zero atomically with read
    final_val = most_recent_val;
  }
  return final_val;
}


double
SmoothingQCongestionMonitor::capacity_ratio() {
  msec_t now = get_msec();
    msec_t tdelta = now - tstamps[v_idx];
  
  if (tdelta >=  SMOOTH_STEP_MS) {
    boost::unique_lock<boost::recursive_mutex> lock(internals);
    
    unsigned my_inserts = write_and_clear(&insertsInPeriod);
    unsigned my_removes = write_and_clear(&removesInPeriod);

    unsigned newQLen = queueLen + my_inserts - my_removes;
    atomic_write32(&queueLen, newQLen);

    v_idx = (v_idx + 1) % WIND_SIZE;
    inserts[v_idx] = my_inserts;
    removes[v_idx] = my_removes;
    tstamps[v_idx] = now;
    
    total_inserts = std::accumulate(inserts.begin(),inserts.end(),0);
    total_removes = std::accumulate(removes.begin(),removes.end(),0);
    long growth = (total_inserts - total_removes);
    long future_queue_size = newQLen + growth  * PROJECT_STEPS / WIND_SIZE;
    double delta_to_achieve = queueTarget - future_queue_size; //positive delta means "ramp up";
              //negative delta means to shrink
              //inserts per timestep would be total_inserts / WIND_SIZE.
                //ratio is (delta_to_achieve / PROJECT_STEPS) / inserts_per_timestep
    if (total_inserts == 0)
      ratio = INFINITY;
    else
      ratio = 1 + ( delta_to_achieve / total_inserts) * (WIND_SIZE / PROJECT_STEPS);
    if (ratio < 0)
      ratio = 0;
    
    msec_t tdelta_full_window =  tstamps[v_idx] - tstamps[(v_idx+1) % WIND_SIZE];
    
    double rate_per_sec = total_inserts *  1000.0 / tdelta_full_window;
    ratio = fmin(ratio, max_per_sec / rate_per_sec);
  }
  return (ratio < downstream_status) ? ratio : downstream_status;
}

msec_t
SmoothingQCongestionMonitor::measurement_time() {
  boost::unique_lock<boost::recursive_mutex> lock(internals);
  if (downstream_status < ratio)
    return downstream_report_time;
  else
    return tstamps[(v_idx+1) % WIND_SIZE];
}
   
string
SmoothingQCongestionMonitor::long_description() {
  boost::unique_lock<boost::recursive_mutex> lock(internals);

  ostringstream buf;
  msec_t tdelta =  tstamps[v_idx] - tstamps[(v_idx+1) % WIND_SIZE];
  buf << "In last " << WIND_SIZE << " timesteps, " << total_inserts
    << " inserts " << total_removes << " removes. Qsize " << queue_length() << "/"<<queueTarget
    << ". TDelta: " << tdelta
    << " Local-ratio: " << ratio << " Downstream: " << downstream_status
//    << " Limit is " << (limit_is_remote() ? "Remote" : "Local");
  return buf.str();
}


} //end namespace
