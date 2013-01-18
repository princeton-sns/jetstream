#ifndef JetStream_queue_congestion_mon_h
#define JetStream_queue_congestion_mon_h

#include <boost/shared_ptr.hpp>
#include "congestion_monitor.h"
#include <boost/interprocess/detail/atomic.hpp>
#include <math.h>

namespace jetstream {

class QueueCongestionMonitor: public CongestionMonitor {
 static const int SAMPLE_INTERVAL_MS = 100;

 private:
  boost::uint32_t queueTarget;
  boost::uint32_t queueLen;
  boost::uint32_t prevQueueLen;
  boost::uint32_t insertsInPeriod;
  mutable boost::mutex internals;
  usec_t lastQueryTS;
  double prevRatio;
  double downstream_status;
  double max_per_sec;
  std::string name;

  
 public:
    QueueCongestionMonitor(uint32_t qTarg, const std::string& nm, double max_per_sec_ = INFINITY):
      queueTarget(qTarg), queueLen(0), prevQueueLen(0), insertsInPeriod(0), lastQueryTS(0),
        prevRatio(INFINITY), downstream_status(INFINITY),max_per_sec(max_per_sec_),name(nm)  { }
    
    virtual double capacity_ratio();
  
  
    int queue_length() {
      int qLen =  boost::interprocess::ipcdetail::atomic_read32(&queueLen);
      return qLen;
    }
  
    virtual ~QueueCongestionMonitor() {};
  
    void report_insert(void * item, uint32_t weight) {
      boost::interprocess::ipcdetail::atomic_add32(&queueLen, weight);
      boost::interprocess::ipcdetail::atomic_add32(&insertsInPeriod, weight);
    }
  
    void report_delete(void * item, uint32_t weight) {
      uint32_t negweight = -weight;  // Do we need a cast here to appease pedantic compilers?
      boost::interprocess::ipcdetail::atomic_add32(&queueLen, negweight);
    }

    void set_downstream_congestion(double d) {
      boost::unique_lock<boost::mutex> lock(internals);
      downstream_status = d;
    }
  
    void set_queue_size(uint32_t s) {
      queueTarget = s;
    }
  
    void set_max_rate(double d) {max_per_sec = d;}

};

}

#endif
