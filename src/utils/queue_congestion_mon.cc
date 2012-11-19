
#include "queue_congestion_mon.h"

namespace jetstream {

double 
QueueCongestionMonitor::capacity_ratio() {
  boost::unique_lock<boost::mutex> lock(internals);
  usec_t now = get_usec();
  
    //only sample every 100 ms
  if (now > lastQueryTS + 100 * 1000) {
    lastQueryTS = now;
    if (queueLen > maxQueue)
      prevRatio = 0.333;
    else
      prevRatio = 3;
  }
  
  return prevRatio < upstream_status ? prevRatio : upstream_status;
}


}