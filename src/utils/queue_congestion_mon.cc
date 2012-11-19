
#include "queue_congestion_mon.h"

namespace jetstream {

bool
QueueCongestionMonitor::is_congested() {
  boost::unique_lock<boost::mutex> lock(internals);
  usec_t now = get_usec();
  
  if (now > lastQueryTS + 100 * 1000) {
    lastQueryTS = now;
    wasCongestedLast = queueLen > maxQueue;
  }
  
  return wasCongestedLast;
}


}