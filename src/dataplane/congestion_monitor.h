#ifndef JetStream_congestion_monitor_h
#define JetStream_congestion_monitor_h

#include <boost/shared_ptr.hpp>

namespace jetstream {

class CongestionMonitor {
  public:
    virtual bool is_congested() = 0;
    virtual ~CongestionMonitor() {};

};


class UncongestedMonitor: public CongestionMonitor {
  public:
    virtual bool is_congested() {
      return false;
    }

};

}
#endif
