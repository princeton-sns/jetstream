#ifndef JetStream_congestion_monitor_h
#define JetStream_congestion_monitor_h

#include <boost/shared_ptr.hpp>
#include "connection.h"

namespace jetstream {

class CongestionMonitor {
  public:
    virtual bool is_congested() = 0;
    virtual ~CongestionMonitor() {};

};

class QueueCongestionMonitor: public CongestionMonitor {
  private:
    const boost::shared_ptr<ClientConnection> out_connection;


  public:
    static const int MAX_QUEUE_BYTES = 1E6;
    virtual bool is_congested() {
      return !out_connection || (out_connection->bytes_queued() > MAX_QUEUE_BYTES);
    }
  
    QueueCongestionMonitor(boost::shared_ptr<ClientConnection> out) : out_connection(out) {}

};

class UncongestedMonitor: public CongestionMonitor {
  public:
    virtual bool is_congested() {
      return false;
    }

};

}
#endif
