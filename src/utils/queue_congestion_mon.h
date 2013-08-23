#ifndef JetStream_queue_congestion_mon_h
#define JetStream_queue_congestion_mon_h

#include <boost/shared_ptr.hpp>
#include "net_congest_mon.h"
#include <boost/interprocess/detail/atomic.hpp>
#include <boost/thread/locks.hpp>
#include <boost/thread/mutex.hpp>
#include <math.h>

namespace jetstream {

class GenericQCongestionMonitor: public NetCongestionMonitor {

   public:
    GenericQCongestionMonitor(uint32_t qTarg, const std::string& nm):
      NetCongestionMonitor(nm), queueTarget(qTarg),queueLen(0) {}

    void set_queue_size(uint32_t s) {
      queueTarget = s;
    }
  
    int queue_length() {
      int qLen =  boost::interprocess::ipcdetail::atomic_read32(&queueLen);
      return qLen;
    }
  
  protected:
    boost::uint32_t queueTarget;
    boost::uint32_t queueLen;
  

};


class QueueCongestionMonitor: public GenericQCongestionMonitor {
 static const int SAMPLE_INTERVAL_MS = 100;

 private:
  boost::uint32_t prevQueueLen;
  boost::uint32_t insertsInPeriod;
  usec_t lastQueryTS;
  double prevRatio;

  //purely for monitoring
  uint32_t prev_inserts;
  int32_t removes;
  
 public:
    QueueCongestionMonitor(uint32_t qTarg, const std::string& nm):
      GenericQCongestionMonitor(qTarg, nm), prevQueueLen(0), insertsInPeriod(0), lastQueryTS(0),
        prevRatio(INFINITY)  { }
    
    virtual double capacity_ratio();
  
    virtual ~QueueCongestionMonitor() {};
  
    virtual void report_insert(void * item, uint32_t weight) {
      boost::interprocess::ipcdetail::atomic_add32(&insertsInPeriod, weight);
      boost::interprocess::ipcdetail::atomic_add32(&queueLen, weight);
    }
  
    virtual void report_delete(void * item, uint32_t weight) {
      uint32_t negweight = -weight;  // Do we need a cast here to appease pedantic compilers?
      boost::interprocess::ipcdetail::atomic_add32(&queueLen, negweight);
    }
  
    virtual std::string long_description();
  
    virtual msec_t measurement_staleness_ms();


};

class SmoothingQCongestionMonitor: public GenericQCongestionMonitor {

  public:

    SmoothingQCongestionMonitor(uint32_t qTarg, const std::string& nm);
  
    virtual double capacity_ratio();

    virtual void report_insert(void * item, uint32_t weight) {
      boost::interprocess::ipcdetail::atomic_add32(&insertsInPeriod, weight);
    }
  
    virtual void report_delete(void * item, uint32_t weight) {
      boost::interprocess::ipcdetail::atomic_add32(&removesInPeriod, weight);
    }
  
    virtual std::string long_description();
  
    virtual msec_t measurement_staleness_ms();

  private:
    boost::uint32_t insertsInPeriod;
    boost::uint32_t removesInPeriod;
    std::vector<int> inserts;
    std::vector<int> removes;
    std::vector<msec_t> tstamps;

    int v_idx;
//    double prevRatio;
    double ratio;
    long total_inserts, total_removes;


};

}

#endif
