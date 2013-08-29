#ifndef JetStream_net_congest_mon_h
#define JetStream_net_congest_mon_h

#include "congestion_monitor.h"
#include <boost/thread/mutex.hpp>
#include <boost/thread/recursive_mutex.hpp>

namespace  jetstream {

class NetCongestionMonitor : public CongestionMonitor {

  protected:
    double downstream_status;
    mutable boost::recursive_mutex internals;
    double max_per_sec;  //bytes per sec
    msec_t downstream_report_time;

    double ratio;
  
  public:
    NetCongestionMonitor(const std::string& n) : CongestionMonitor(n),
            downstream_status(INFINITY),  max_per_sec(INFINITY), downstream_report_time(0),
            ratio(10) {}
  
    virtual void report_insert(void * item, uint32_t weight) = 0;
  
    virtual void report_delete(void * item, uint32_t weight) {}
  
    virtual void end_of_window(int window_ms, msec_t start_time) {}

    void set_downstream_congestion(double d, msec_t report_time) {
      boost::unique_lock<boost::recursive_mutex> lock(internals);
      downstream_status = d;
      downstream_report_time = report_time;
    }
  
    void set_max_rate(double d) {max_per_sec = d;}
  
    virtual msec_t get_window_start() { return 0; }
  
    virtual void new_window_start() { }
  
    virtual std::string long_description() = 0;
  
    bool limit_is_remote() {
      return downstream_status < ratio;
    }
  
};

}

#endif
