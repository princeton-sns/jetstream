
#include "queue_congestion_mon.h"
#include "window_congest_mon.h"
#include <glog/logging.h>
#include <algorithm> 

using namespace boost::interprocess::ipcdetail;
using namespace ::std;

namespace jetstream {

const int N_TO_LOG = 211;

double
QueueCongestionMonitor::capacity_ratio() {
  boost::unique_lock<boost::mutex> lock(internals);
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


    int32_t queueDelta = readQLen - prevQueueLen; //negative implies queue is shrinking
    int32_t availRoom = queueTarget - readQLen - queueDelta; //negative 'availRoom' implies we need to cut the rate
    if (  ((int32_t)inserts ) < -availRoom) // Signed compare: queue won't drain fast enough enough room even with no inserts
      prevRatio = 0; //should stop sends altogether to let things drain
    else
      prevRatio = availRoom /(double) inserts + 1.0;

    double rate_per_sec = inserts * 1000.0 / SAMPLE_INTERVAL_MS;
    prevRatio = fmin(prevRatio, max_per_sec / rate_per_sec);
    int32_t removes = inserts - queueDelta;
    if (removes < 0) {
      LOG(WARNING) << "Shouldn't have data leaking out of " << name()    
        <<  ". Inserts: " << inserts <<" queueDelta: " <<queueDelta << " readQLen: " << readQLen << " prevQueueLen: "<<prevQueueLen;
      removes = 0;
    }

    result = prevRatio < downstream_status ? prevRatio : downstream_status;
    LOG_IF_EVERY_N(INFO, readQLen > 0 || inserts > 0 || prevRatio == 0 , N_TO_LOG) <<
        "(logged every "<<N_TO_LOG<<") Queue for " << name() << ": " << inserts <<
         " inserts (max is " << max_per_sec << "); " << removes  <<" removes. Queue length "
          << readQLen << "/" << queueTarget << ". Space Ratio is " << prevRatio <<
            ", downstream is " << downstream_status<< " and final result is " << result;
    LOG_IF(FATAL, prevRatio < 0) << "ratio should never be negative";
    prevQueueLen = readQLen;
  } else
    result = prevRatio < downstream_status ? prevRatio : downstream_status;

  return result;
}

WindowCongestionMonitor::WindowCongestionMonitor(const std::string& name):
  NetCongestionMonitor(name), last_ratio(INFINITY), window_start_time(0),
      last_window_end(get_msec()), bytes_in_window(0) {
}



void
WindowCongestionMonitor::end_of_window(int window_data_ms, msec_t processing_start_time) {
  boost::unique_lock<boost::mutex> lock(internals);
  msec_t now = get_msec();

  if (processing_start_time != 0) {
    msec_t window_processtime_ms = now - processing_start_time;
    msec_t window_availtime_ms = now - last_window_end;
    
    double window_ratio = double(window_data_ms) / window_processtime_ms;
    double bytes_per_sec = bytes_in_window * 1000.0 / window_availtime_ms;
    last_ratio = std::min(window_ratio, max_per_sec / bytes_per_sec);
//    window_start_time = 0;
    LOG(INFO) << "End of window@ " << (now/1000) << " Capacity ratio at " << name() << " is now "
      << last_ratio <<  ". Durations were " << window_processtime_ms << "/"
      << window_availtime_ms  <<" and window size was " << window_data_ms
      <<", saw " << bytes_per_sec << " bytes/sec. Final ratio " <<  fmin(downstream_status, last_ratio);
  } else { //no data in window; we are therefore UNCONSTRAINED
    last_ratio = 10;
  }
  bytes_in_window = 0;
  last_window_end = now;
  
}

double
WindowCongestionMonitor::capacity_ratio() {
  boost::unique_lock<boost::mutex> lock(internals);    
  return fmin(downstream_status, last_ratio);
  //downstream status can change asynchronously. So can't roll it into last_ratio
}               

void
WindowCongestionMonitor::report_insert(void * item, uint32_t weight) {
  if (window_start_time == 0) {
//      LOG(INFO) << "SAW A SEND";
    boost::unique_lock<boost::mutex> lock(internals);
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
  
  
} //end namespace
