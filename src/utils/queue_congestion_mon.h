#ifndef JetStream_queue_congestion_mon_h
#define JetStream_queue_congestion_mon_h

#include <boost/shared_ptr.hpp>
#include "congestion_monitor.h"
#include <boost/interprocess/detail/atomic.hpp>
#include <math.h>

namespace jetstream {

class QueueCongestionMonitor: public CongestionMonitor {

 private:
  boost::uint32_t maxQueue;
  boost::uint32_t queueLen;
  boost::uint32_t prevQueueLen;
  boost::uint32_t insertsInPeriod;
  mutable boost::mutex internals;
  usec_t lastQueryTS;
  double prevRatio;
  double upstream_status;

  
 public:
    QueueCongestionMonitor(boost::uint32_t max_q):
      maxQueue(max_q), queueLen(0), prevQueueLen(0), insertsInPeriod(0), lastQueryTS(0),
        prevRatio(INFINITY), upstream_status(INFINITY)  { }


    
    virtual double capacity_ratio();
  
    virtual ~QueueCongestionMonitor() {};
  
    void report_insert(void * item, uint32_t weight) {
      boost::interprocess::ipcdetail::atomic_add32(&queueLen, weight);
      boost::interprocess::ipcdetail::atomic_add32(&insertsInPeriod, weight);
    }
  
    void report_delete(void * item, uint32_t weight) {
      boost::interprocess::ipcdetail::atomic_add32(&queueLen, -weight);
    }

    void set_upstream_congestion(double d) {
      boost::unique_lock<boost::mutex> lock(internals);
      upstream_status = d;
    }
  

};

}

#endif
