#ifndef CHAINED_QUEUE_MON_R5H9TXM9
#define CHAINED_QUEUE_MON_R5H9TXM9

#include "queue_congestion_mon.h"

namespace jetstream {

class ChainedQueueMonitor: public  QueueCongestionMonitor {
  
  public:
    boost::shared_ptr<CongestionMonitor> dest;

    
  ChainedQueueMonitor(uint32_t qTarg, const std::string& nm, double max_per_sec_ = INFINITY):
  QueueCongestionMonitor(qTarg, nm, max_per_sec_) {}

  void set_next_monitor(boost::shared_ptr<CongestionMonitor> next)
  {
    dest=next;
  }

  virtual double capacity_ratio() {
    if(dest)
    {
      double downstream = dest->capacity_ratio();
      set_downstream_congestion(downstream);
    }

    return QueueCongestionMonitor::capacity_ratio();
  }
  
};
} /* jetstream */

#endif /* end of include guard: CHAINED_QUEUE_MON_R5H9TXM9 */
