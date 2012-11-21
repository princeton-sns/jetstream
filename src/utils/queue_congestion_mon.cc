
#include "queue_congestion_mon.h"
#include <glog/logging.h>

using namespace boost::interprocess::ipcdetail;

namespace jetstream {

double 
QueueCongestionMonitor::capacity_ratio() {
  boost::unique_lock<boost::mutex> lock(internals);
  usec_t now = get_usec();
  
    //only sample every 100 ms
  if (now > lastQueryTS + 100 * 1000) {
//    usec_t sample_period = now - lastQueryTS;
    lastQueryTS = now;
  
    uint32_t inserts = atomic_read32(&insertsInPeriod);
    uint32_t newi  = inserts;
    while ( ( newi = atomic_cas32(&insertsInPeriod, 0, newi)) != 0)
      inserts = newi;
    
    int32_t queueDelta = queueLen - prevQueueLen; //negative implies queue is shrinking
    int32_t availRoom = maxQueue/2 - queueLen - queueDelta; //negative 'availRoom' implies we need to cut the rate
    if (  ((int32_t)inserts ) < -availRoom) // Signed compare: queue won't drain fast enough enough room even with no inserts
      prevRatio = 0; //should stop sends altogether to let things drain
    else
      prevRatio = availRoom /(double) inserts + 1.0;
    
    LOG_IF_EVERY_N(INFO, queueLen > 0 || inserts > 0 ||prevRatio == 0 , 20) << "Queue for " << name << ": " << inserts <<
         " inserts; queue length " << queueLen << ". Space Ratio is " << prevRatio << ", upstream is " << upstream_status;
    LOG_IF(FATAL, prevRatio < 0) << "ratio should never be negative";
    prevQueueLen = queueLen;
  }
  
  return prevRatio < upstream_status ? prevRatio : upstream_status;
}


}