
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

  double result;
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
      prevRatio = 0; //should stop sends altogether to let things drain
    else
      prevRatio = availRoom /(double) inserts + 1.0;

    double rate_per_sec = inserts * 1000.0 / SAMPLE_INTERVAL_MS;
    prevRatio = fmin(prevRatio, max_per_sec / rate_per_sec);
    removes = inserts - queueDelta;
    if (removes < 0) {
      LOG(WARNING) << "Shouldn't have data leaking out of " << name()    
        <<  ". Inserts: " << inserts <<" queueDelta: " <<queueDelta << " readQLen: " << readQLen << " prevQueueLen: "<<prevQueueLen;
      removes = 0;
    }

    result = prevRatio < downstream_status ? prevRatio : downstream_status;
    LOG_IF_EVERY_N(INFO, readQLen > 0 || inserts > 0 || prevRatio == 0 , N_TO_LOG) <<
 //   LOG(INFO) <<
        "(logged every "<<N_TO_LOG<<") Queue for " << name() << ": " << long_description();
    LOG_IF(FATAL, prevRatio < 0) << "ratio should never be negative";
    prevQueueLen = readQLen;
  } else
    result = prevRatio < downstream_status ? prevRatio : downstream_status;

  return result;
}


std::string
QueueCongestionMonitor::long_description() {
  boost::unique_lock<boost::recursive_mutex> lock(internals);

  ostringstream buf;
//  int32_t removes = insertsInPeriod + prevQueueLen - queueLen;
  
  double result = prevRatio < downstream_status ? prevRatio : downstream_status;

  buf << prev_inserts << " inserts (configured max rate is " << max_per_sec << "); " << removes
      <<" removes. Final queue length " << prevQueueLen << "/" << queueTarget <<
      ". Space Ratio is " << prevRatio <<
            ", downstream is " << downstream_status<< " and final result is " << result;
  return buf.str();
}


msec_t
QueueCongestionMonitor::measurement_staleness_ms() {
  if (downstream_status < lastQueryTS)
    return get_msec() - downstream_report_time;
  else
    return get_msec() - lastQueryTS;
}



WindowCongestionMonitor::WindowCongestionMonitor(const std::string& name):
  NetCongestionMonitor(name), last_ratio(INFINITY), window_start_time(0),
      last_window_end(get_msec()), bytes_in_window(0) {
}


void
WindowCongestionMonitor::end_of_window(int window_data_ms, msec_t processing_start_time) {
  boost::unique_lock< boost::recursive_mutex> lock(internals);
  msec_t now = get_msec();

  if (processing_start_time != 0) {
    msec_t window_processtime_ms = now - processing_start_time;
    msec_t window_availtime_ms = now - last_window_end;
    
    double window_ratio = double(window_data_ms) / window_processtime_ms;
    double bytes_per_sec = bytes_in_window * 1000.0 / window_availtime_ms;
    last_ratio = std::min(window_ratio, max_per_sec / bytes_per_sec);
//    window_start_time = 0;
    LOG_EVERY_N(INFO, 20) << long_description() <<". Nominal window size was " << window_data_ms;
  } else { //no data in window; we are therefore UNCONSTRAINED
    last_ratio = 10;
  }
  bytes_in_window = 0;
  last_window_end = now;
  window_start_time = 0;
  
}

double
WindowCongestionMonitor::capacity_ratio() {
  boost::unique_lock<boost::recursive_mutex> lock(internals);    
  return fmin(downstream_status, last_ratio);
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
WindowCongestionMonitor::measurement_staleness_ms() {
  if (downstream_status < last_ratio)
    return get_msec() - downstream_report_time;
  else
    return get_msec() - last_window_end;
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
      << last_ratio <<  ". Durations were " << window_processtime_ms << "/"
      << window_availtime_ms  // <<". Nominal window size was " << window_data_ms
      <<", saw " << bytes_per_sec << " bytes/sec. Final ratio " <<  fmin(downstream_status, last_ratio);

  return buf.str();
}

static const unsigned WIND_SIZE = 4;
static const unsigned PROJECT_STEPS = 4;

SmoothingQCongestionMonitor::SmoothingQCongestionMonitor(uint32_t qTarg, const std::string& nm): 
      GenericQCongestionMonitor(qTarg, nm), insertsInPeriod(0), removesInPeriod(0),
        v_idx(0), lastQueryTS(0)  {
  for (unsigned i = 0; i < WIND_SIZE; ++i) {
    inserts.push_back(0);
    removes.push_back(0);
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
  
  boost::unique_lock<boost::recursive_mutex> lock(internals);  
  
  unsigned my_inserts = write_and_clear(&insertsInPeriod);
  unsigned my_removes = write_and_clear(&removesInPeriod);

  unsigned newQLen = queueLen + my_inserts - my_removes;
  atomic_write32(&queueLen, newQLen);
  inserts[v_idx] = my_inserts;
  removes[v_idx] = my_removes;
  v_idx = (v_idx + 1) % WIND_SIZE;
  
  total_inserts = std::accumulate(inserts.begin(),inserts.end(),0);
  total_removes = std::accumulate(removes.begin(),removes.end(),0);
  long growth_per_timestep = (total_inserts - total_removes) / WIND_SIZE;
  long future_queue_size = newQLen + growth_per_timestep * PROJECT_STEPS;
  long delta_to_achieve = queueTarget - future_queue_size; //positive delta means "ramp up";
            //negative delta means to shrink
            //inserts per timestep would be total_inserts / WIND_SIZE.
              //ratio is (delta_to_achieve / PROJECT_STEPS) / inserts_per_timestep
  if (total_inserts == 0)
    ratio = INFINITY;
  else
    ratio = ( delta_to_achieve / total_inserts) * (WIND_SIZE / PROJECT_STEPS);
  
//  double rate_per_sec = inserts * 1000.0 / tdelta;
//  ratio = fmin(ratio, max_per_sec / rate_per_sec);
  
  
  return ratio;
}

msec_t
SmoothingQCongestionMonitor::measurement_staleness_ms() {
  return 0;
}
   
string
SmoothingQCongestionMonitor::long_description() {
  boost::unique_lock<boost::recursive_mutex> lock(internals);

  ostringstream buf;
  buf << "In last " << WIND_SIZE << " timesteps, " << total_inserts
    << " inserts " << total_removes << "removes. Qsize " << queue_length() << " Ratio is " << ratio;
  return buf.str();
}


} //end namespace
