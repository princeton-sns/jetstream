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
  msec_t data_conn_wait;      //wait for a dataplane connection to respond. Should probably be ~minutes, not seconds?
  size_t send_queue_size; //max size of the send queue
  std::string cube_db_host;
  std::string cube_db_user;
  std::string cube_db_pass;
  std::string cube_db_name;
  u_int cube_processor_threads;
  u_int cube_congestion_process_limit;
  u_int cube_congestion_flush_limit;
  bool cube_mysql_innodb;
  bool cube_mysql_engine_memory;
  bool cube_mysql_transactions;
  u_int cube_mysql_insert_batch_pw2;
  u_int cube_mysql_query_batch_pw2;
  u_int cube_max_stage;
  unsigned connection_buffer_size;
  
  NodeConfig (); //moved ctor contents to node.cc to allow changes without recompiling

};

};

#endif /* _node_config_H_ */
