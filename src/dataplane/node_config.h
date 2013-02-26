#ifndef _node_config_H_
#define _node_config_H_

#include "js_defs.h"

namespace jetstream {

class NodeConfig {
 public:
  std::string config_file;
  std::vector<std::pair<std::string, port_t> > controllers; // hostname, port
  std::pair<std::string, port_t> dataplane_ep;  // hostname, port
  port_t webinterface_port;     // Host byte order
  msec_t heartbeat_time;       // Time between heartbeats, in miliseconds
  u_int  thread_pool_size;
  msec_t data_conn_wait;      //wait for a dataplane connection to respond. Should probably be minutes, not seconds?
  size_t sendQueueSize; //max size of the send queue
  std::string cube_db_host;
  std::string cube_db_user;
  std::string cube_db_pass;
  std::string cube_db_name;
  u_int cube_processor_threads;
  NodeConfig ()
    : dataplane_ep("0.0.0.0", 0), webinterface_port (0),
    heartbeat_time (0), thread_pool_size (1), data_conn_wait(5000),sendQueueSize(1E5),
    cube_db_host("localhost"), cube_db_user("root"), cube_db_pass(""), cube_db_name("test_cube"),
    cube_processor_threads(1)
    {}
};

};

#endif /* _node_config_H_ */
