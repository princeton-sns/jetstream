
#ifndef JetStream_window_congest_mon_h
#define JetStream_window_congest_mon_h

#include <boost/shared_ptr.hpp>
//#include <boost/thread.hpp>
#include "js_utils.h"
#include "congestion_monitor.h"
#include "net_congest_mon.h"

namespace jetstream {


class WindowCongestionMonitor: public NetCongestionMonitor {

  protected:
    volatile double last_ratio;
    volatile msec_t window_start_time;
    unsigned bytes_in_window;
  
  public:
  
    WindowCongestionMonitor(const std::string& name): NetCongestionMonitor(name), last_ratio(INFINITY), window_start_time(0) {}
  
    virtual double capacity_ratio() {
      return last_ratio;
    }
  
    virtual void report_insert(void * item, uint32_t weight);
  
    virtual void end_of_window(int window_ms, msec_t start_time);
  
    virtual msec_t get_window_start() { return window_start_time; }
  
    virtual void new_window_start() { window_start_time = 0; }
};

}

#endif
