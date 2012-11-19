#ifndef JetStream_queue_congestion_mon_h
#define JetStream_queue_congestion_mon_h

#include <boost/shared_ptr.hpp>
#include "congestion_monitor.h"
#include <boost/interprocess/detail/atomic.hpp>

namespace jetstream {

class QueueCongestionMonitor: public CongestionMonitor {

 private:
  boost::uint32_t maxQueue;
  boost::uint32_t queueLen;
  mutable boost::mutex internals;
  usec_t lastQueryTS;
  bool wasCongestedLast;

  
 public:
    QueueCongestionMonitor(boost::uint32_t max_q):
      maxQueue(max_q), queueLen(0), lastQueryTS(0),wasCongestedLast(0)  { }


    virtual bool is_congested();
  
    virtual ~QueueCongestionMonitor() {};
  
    void report_insert(void * item) {
      boost::interprocess::ipcdetail::atomic_inc32(&queueLen);
    }
  
    void report_delete(void * item) {
      boost::interprocess::ipcdetail::atomic_dec32(&queueLen);
    }

  

};

}

#endif
