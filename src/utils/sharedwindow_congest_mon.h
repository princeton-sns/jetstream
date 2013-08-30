#ifndef __JetStream__sharedwindow_congest_mon__
#define __JetStream__sharedwindow_congest_mon__

#include "window_congest_mon.h"

namespace jetstream {


class SharedWindowMonitor;


class WindowMonFacade: public WindowCongestionMonitor {

  public:
    WindowMonFacade(const std::string& name, double smoothing_prev_value);

    virtual double capacity_ratio();
  
//    virtual void end_of_window(int window_ms, msec_t start_time);
  
  protected:
    SharedWindowMonitor * underlying;

};


class SharedWindowMonitor {
  public:
    double update_from_mon(WindowMonFacade * src, double update);
    SharedWindowMonitor():last_period_end(0),ratio_this_period(10) { }
    
    
  protected:
    msec_t last_period_end;
    std::vector<double> measurements_in_period;
    double ratio_this_period;

/*    std::map<WindowMonFacade *, double> updates_in_period;
 
    unsigned windows_in_period;
    double totalwind_in_period;
    double min_congest_in_period;
    msec_t max_window_in_period;
 */
    mutable boost::mutex internals;

};

}

#endif /* defined(__JetStream__sharedwindow_congest_mon__) */
