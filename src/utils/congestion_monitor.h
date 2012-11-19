#ifndef JetStream_congestion_monitor_h
#define JetStream_congestion_monitor_h

#include <boost/shared_ptr.hpp>
#include <boost/thread.hpp>
#include "js_utils.h"

namespace jetstream {

class CongestionMonitor {
  public:
    /** Returns the ratio between the capacity and the current send rate.
      Upstream sources should scale their send rate by this ratio
    */
    virtual double capacity_ratio() = 0;
  
    bool is_congested() {return capacity_ratio() < 1;}

    virtual ~CongestionMonitor() {};
  
    void wait_for_space() {
      while (is_congested()) {
        boost::this_thread::yield();
        js_usleep(100 * 1000);
      }
    }


};


class UncongestedMonitor: public CongestionMonitor {
  public:
    virtual double capacity_ratio() {
      return 10; // can ramp up exponentially with this ratio
    }

};


}
#endif
