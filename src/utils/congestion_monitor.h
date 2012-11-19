#ifndef JetStream_congestion_monitor_h
#define JetStream_congestion_monitor_h

#include <boost/shared_ptr.hpp>
#include <boost/thread.hpp>
#include "js_utils.h"

namespace jetstream {

class CongestionMonitor {
  public:
    virtual bool is_congested() = 0;
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
    virtual bool is_congested() {
      return false;
    }

};


}
#endif
