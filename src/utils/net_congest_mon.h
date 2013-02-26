#ifndef JetStream_net_congest_mon_h
#define JetStream_net_congest_mon_h

#include "congestion_monitor.h"

namespace  jetstream {

class NetCongestionMonitor : public CongestionMonitor {

  protected:
    double downstream_status;
    mutable boost::mutex internals;
    double max_per_sec;  //bytes per sec

  
  public:
    NetCongestionMonitor(const std::string& n) : CongestionMonitor(n),
            downstream_status(INFINITY),  max_per_sec(INFINITY) {}
  
    virtual void report_insert(void * item, uint32_t weight) = 0;
  
    virtual void report_delete(void * item, uint32_t weight) {}
  
    virtual void end_of_window(int window_ms, msec_t start_time) {}

    void set_downstream_congestion(double d) {
      boost::unique_lock<boost::mutex> lock(internals);
      downstream_status = d;
    }
  
    void set_max_rate(double d) {max_per_sec = d;}
  
    virtual msec_t get_window_start() { return 0; }
  
    virtual void new_window_start() { }
};

}

#endif
