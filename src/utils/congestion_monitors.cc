
#include "queue_congestion_mon.h"
#include "window_congest_mon.h"
#include <glog/logging.h>

using namespace boost::interprocess::ipcdetail;

namespace jetstream {

double
QueueCongestionMonitor::capacity_ratio() {
  boost::unique_lock<boost::mutex> lock(internals);
  usec_t now = get_usec();

  double result;
    //only sample every 100 ms
  if (now > lastQueryTS + SAMPLE_INTERVAL_MS * 1000) {
//    usec_t sample_period = now - lastQueryTS;
    lastQueryTS = now;

    uint32_t readQLen = atomic_read32(&queueLen); //a fixed value

    uint32_t inserts = atomic_read32(&insertsInPeriod);
    uint32_t newi  = inserts;
    while ( ( newi = atomic_cas32(&insertsInPeriod, 0, newi)) != 0) //reset to zero atomically with read
      inserts = newi;


    int32_t queueDelta = readQLen - prevQueueLen; //negative implies queue is shrinking
    int32_t availRoom = queueTarget - readQLen - queueDelta; //negative 'availRoom' implies we need to cut the rate
    if (  ((int32_t)inserts ) < -availRoom) // Signed compare: queue won't drain fast enough enough room even with no inserts
      prevRatio = 0; //should stop sends altogether to let things drain
    else
      prevRatio = availRoom /(double) inserts + 1.0;

    double rate_per_sec = inserts * 1000.0 / SAMPLE_INTERVAL_MS;
    prevRatio = fmin(prevRatio, max_per_sec / rate_per_sec);
    int32_t removes = inserts - queueDelta;
    LOG_IF(FATAL, removes < 0) << "Shouldn't have data leaking out of queue inserts: " << inserts <<" queueDelta: " <<queueDelta;
    result = prevRatio < downstream_status ? prevRatio : downstream_status;
    LOG_IF_EVERY_N(INFO, readQLen > 0 || inserts > 0 ||prevRatio == 0 , 21) << "(logged every 21) Queue for " << name() << ": " << inserts <<
         " inserts (max is " << max_per_sec << "); " << removes  <<" removes. Queue length " << readQLen << "/" << queueTarget << ". Space Ratio is " << prevRatio << ", downstream is " << downstream_status<< " and final result is " << result;
    LOG_IF(FATAL, prevRatio < 0) << "ratio should never be negative";
    prevQueueLen = readQLen;
  } else
    result = prevRatio < downstream_status ? prevRatio : downstream_status;

  return result;
}


void
WindowCongestionMonitor::end_of_window(int window_ms, msec_t true_start_time) {
  if (true_start_time != 0) {
    last_ratio = window_ms / double(get_msec() - true_start_time);
    window_start_time = 0;
    LOG(INFO) << "End of window! Congestion level at " << name() << " is now " << last_ratio ;
  }
}



}
