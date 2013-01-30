
#ifndef JetStream_window_congest_mon_h
#define JetStream_window_congest_mon_h

#include <boost/shared_ptr.hpp>
//#include <boost/thread.hpp>
#include "js_utils.h"
#include "congestion_monitor.h"


namespace jetstream {


class WindowCongestionMonitor: public CongestionMonitor {

  protected:
    double last_ratio;
    msec_t window_start_time;
  
  public:
  
    WindowCongestionMonitor(): last_ratio(INFINITY), window_start_time(0) {}
  
    virtual double capacity_ratio() {
      return last_ratio;
    }
  
    void tuple() {
      if (window_start_time == 0) {
        window_start_time = get_msec();
      }
    }
  
    void end_of_window(int window_ms) {
      if (window_start_time != 0) {
        last_ratio = window_ms / double(get_msec() - window_start_time); 
        window_start_time = 0;
      }
    }
};

}

#endif
