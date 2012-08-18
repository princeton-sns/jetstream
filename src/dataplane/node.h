#ifndef _node_H_
#define _node_H_

#include <sys/types.h>
#include <boost/thread.hpp>

#include "js_utils.h"
#include "jetstream_types.pb.h"
#include "dataplaneoperator.h"
#include "dataplane_operator_loader.h"
#include "client_conn.h"
#include "cube_manager.h"
#include "liveness_manager.h"

namespace jetstream {
  
class net_interface;


class NodeConfig {
 public:
  std::string config_file;
  std::vector<std::pair<std::string, port_t> > controllers; // Domain, port
  port_t controlplane_myport;  // Host byte order
  port_t dataplane_myport;     // Host byte order
  msec_t heartbeat_time;
  u_int  thread_pool_size;
  NodeConfig () 
    : controlplane_myport (0), dataplane_myport (0), 
    heartbeat_time (0), thread_pool_size (0)
    {}
};


#if 0  
class ConnectionToController : public WorkerConnHandler {
 public:
  ConnectionToController (boost::asio::io_service& io_service,
			  boost::asio::ip::tcp::resolver::iterator endpoint_iterator)
    : WorkerConnHandler (io_service, endpoint_iterator) {}

  virtual ~ConnectionToController () {}
  virtual void process_message (char *buf, size_t sz);
  
};
  

class hb_loop {
 private:
  boost::shared_ptr<ConnectionToController> uplink;
 public:
  hb_loop (boost::shared_ptr<ConnectionToController> t) : uplink (t) {}
  //could potentially add a ctor here with some args
  void operator () ();
};
#endif

struct operator_id_t {
  int32_t computation_id; // which computation
  int32_t task_id; // which operator in the computation
  bool operator< (const operator_id_t& rhs) const {
    return computation_id < rhs.computation_id 
      || task_id < rhs.task_id;
  }
    
  operator_id_t (int32_t c, int32_t t) : computation_id (c), task_id (t) {}
  operator_id_t () : computation_id (0), task_id (0) {}
};
  
class Node {
 private:
  NodeConfig config;
  CubeManager cube_mgr;
  boost::shared_ptr<boost::asio::io_service> iosrv;
  boost::shared_ptr<ClientConnectionManager> conn_mgr; 

  boost::shared_ptr<LivenessManager> liveness_mgr;
  //boost::shared_ptr<ConnectionToController> uplink;

  std::vector<boost::shared_ptr<boost::thread> > threads;

  DataPlaneOperatorLoader operator_loader;  
  std::map<operator_id_t, boost::shared_ptr<jetstream::DataPlaneOperator> > operators;

  void controller_connected (boost::shared_ptr<ClientConnection> conn,
			     boost::system::error_code error);
  
 public:
  Node (const NodeConfig &conf);
  ~Node ();

  void run ();
  void stop ();

  //void connect_to_master ();
  //void start_heartbeat_thread();

  boost::shared_ptr<DataPlaneOperator>
    get_operator (operator_id_t name) { return operators[name]; }
  
  boost::shared_ptr<DataPlaneOperator>
    create_operator (std::string op_typename, operator_id_t name);
  
  void handle_alter (AlterTopo t); //FIXME: this may be refactored away. For now
  //it handles incoming alter messages, and starts/stops operators

};

//const int HB_INTERVAL = 5; //seconds
  
}


#endif /* _node_H_ */
