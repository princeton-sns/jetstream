#ifndef JetStream_congestion_monitor_h
#define JetStream_congestion_monitor_h

#include <boost/shared_ptr.hpp>
#include <boost/thread.hpp>

namespace jetstream {

class CongestionMonitor {
  public:
    virtual bool is_congested() = 0;
    virtual ~CongestionMonitor() {};
  
  
    void wait_for_space() {
      while (is_congested()) {
        boost::this_thread::yield();
        boost::this_thread::sleep(boost::posix_time::milliseconds(100));
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
