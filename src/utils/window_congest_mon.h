
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
    msec_t last_window_end;
    volatile unsigned bytes_in_window;
  
  public:
  
    WindowCongestionMonitor(const std::string& name);
  
    virtual double capacity_ratio();
  
    virtual void report_insert(void * item, uint32_t weight);
  
    virtual void end_of_window(int window_ms, msec_t start_time);

    virtual void end_of_window(int window_ms) {
      end_of_window(window_ms, get_window_start());
    }

    virtual msec_t get_window_start() { return window_start_time; }
  
    virtual void new_window_start() { window_start_time = 0; }
  
    virtual msec_t measurement_staleness_ms();
  
};

}

#endif
