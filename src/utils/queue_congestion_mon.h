#ifndef JetStream_queue_congestion_mon_h
#define JetStream_queue_congestion_mon_h

#include <boost/shared_ptr.hpp>
#include "net_congest_mon.h"
#include <boost/interprocess/detail/atomic.hpp>
#include <boost/thread/locks.hpp>
#include <boost/thread/mutex.hpp>
#include <math.h>

namespace jetstream {

class QueueCongestionMonitor: public NetCongestionMonitor {
 static const int SAMPLE_INTERVAL_MS = 100;

 private:
  boost::uint32_t queueTarget;
  boost::uint32_t queueLen;
  boost::uint32_t prevQueueLen;
  boost::uint32_t insertsInPeriod;
  usec_t lastQueryTS;
  double prevRatio;

  
 public:
    QueueCongestionMonitor(uint32_t qTarg, const std::string& nm):
      NetCongestionMonitor(nm), queueTarget(qTarg), queueLen(0), prevQueueLen(0), insertsInPeriod(0), lastQueryTS(0),
        prevRatio(INFINITY)  { }
    
    virtual double capacity_ratio();
  
  
    int queue_length() {
      int qLen =  boost::interprocess::ipcdetail::atomic_read32(&queueLen);
      return qLen;
    }
  
    virtual ~QueueCongestionMonitor() {};
  
    virtual void report_insert(void * item, uint32_t weight) {
      boost::interprocess::ipcdetail::atomic_add32(&insertsInPeriod, weight);
      boost::interprocess::ipcdetail::atomic_add32(&queueLen, weight);
    }
  
    virtual void report_delete(void * item, uint32_t weight) {
      uint32_t negweight = -weight;  // Do we need a cast here to appease pedantic compilers?
      boost::interprocess::ipcdetail::atomic_add32(&queueLen, negweight);
    }

    void set_queue_size(uint32_t s) {
      queueTarget = s;
    }
  
    virtual std::string long_description();  

};

}

#endif
