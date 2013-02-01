
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
    double last_ratio;
    msec_t window_start_time;
  
  public:
  
    WindowCongestionMonitor(const std::string& name): NetCongestionMonitor(name), last_ratio(INFINITY), window_start_time(0) {}
  
    virtual double capacity_ratio() {
      return last_ratio;
    }
  
    virtual void report_insert(void * item, uint32_t weight) {
      if (window_start_time == 0) {
        window_start_time = get_msec();
      }
    }
  
    virtual void end_of_window(int window_ms) {
      if (window_start_time != 0) {
        last_ratio = window_ms / double(get_msec() - window_start_time); 
        window_start_time = 0;
      }
    }
};

}

#endif