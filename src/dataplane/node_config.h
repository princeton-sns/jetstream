#ifndef _node_config_H_
#define _node_config_H_

#include "js_defs.h"

namespace jetstream {

class NodeConfig {
 public:
  std::string config_file;
  std::vector<std::pair<std::string, port_t> > controllers; // Domain, port
  port_t controlplane_myport;  // Host byte order
  port_t dataplane_myport;     // Host byte order
  port_t webinterface_port;     // Host byte order
  msec_t heartbeat_time;       // Time between heartbeats, in miliseconds
  u_int  thread_pool_size;
  msec_t data_conn_wait;      //wait for a dataplane connection to respond. Should probably be minutes, not seconds?
  size_t sendQueueSize; //max size of the send queue
  NodeConfig () 
    : controlplane_myport (0), dataplane_myport (0), webinterface_port (0),
    heartbeat_time (0), thread_pool_size (1), data_conn_wait(5000),sendQueueSize(1E6)
    {}
};

};

#endif /* _node_config_H_ */
