#ifndef _nodedataplane_H_
#define _nodedataplane_H_

#include <sys/types.h>

#include "js_utils.h"
#include "jetstream_types.pb.h"
#include "jetstream_dataplane.pb.h"
#include "worker_conn_handler.h"
#include "dataplaneoperator.h"
#include "dataplane_operator_loader.h"


namespace jetstream {
  
class net_interface;
  
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
  
struct operator_id_t {
  int32_t computation_id; //which computation
  int32_t task_id; //which operator in the computation
  bool operator<(const operator_id_t& rhs) const {
    return computation_id < rhs.computation_id ||
    task_id < rhs.task_id;
  }
}; 
  
class NodeDataPlane {
 private:
  bool alive;
  boost::shared_ptr<boost::asio::io_service> iosrv;
  boost::shared_ptr<ConnectionToController> uplink;
  //ClientConnectionPool pool;
  std::map<operator_id_t, boost::shared_ptr<jetstream::DataPlaneOperator> > operators;
 public:
  NodeDataPlane();
  ~NodeDataPlane();
  void connect_to_master ();
  void start_heartbeat_thread();
  
  boost::shared_ptr<jetstream::DataPlaneOperator>
      get_operator(operator_id_t name)  {return operators[name];}
  
  boost::shared_ptr<jetstream::DataPlaneOperator>
  create_operator(std::string op_typename, operator_id_t name);
  

  void handle_alter(AlterTopo t); //FIXME: this may be refactored away. For now
  //it handles incoming alter messages, and starts/stops operators

};

const int HB_INTERVAL = 5; //seconds
  
}


#endif /* _nodedataplane_H_ */
