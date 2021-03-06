#ifndef _node_H_
#define _node_H_

#include <sys/types.h>
#include <boost/thread.hpp>

#include "js_utils.h"
#include "jetstream_types.pb.h"
#include "dataplane_operator_loader.h"
#include "connection.h"
#include "cube_manager.h"
#include "liveness_manager.h"
#include "dataplane_comm.h"
#include "node_web_interface.h"
#include "node_config.h"
#include "congest_policy.h"
#include "counter.h"


namespace jetstream {
  
class net_interface;

class Node {
  friend class NodeWebInterface;

 private:
  NodeConfig config;
  boost::shared_ptr<boost::asio::io_service> iosrv;
  boost::asio::io_service::work iosrv_work; 
  boost::shared_ptr<ConnectionManager> connMgr;
  LivenessManager livenessMgr;
  NodeWebInterface  webInterface;
  CubeManager cubeMgr;
  DataplaneConnManager dataConnMgr;
  boost::shared_ptr<ServerConnection> listeningSock;
  std::vector<boost::shared_ptr<ClientConnection> > controllers;
  mutable boost::mutex threadpoolLock; // a mutex to make sure concurrent thread starts/stops
            //don't interfere
  mutable boost::shared_mutex operatorTableLock; // protects list of operators
  mutable boost::mutex jobStartLock;
  
  boost::condition_variable startStopCond;
                                                    
  std::vector<boost::shared_ptr<boost::thread> > threads;

  DataPlaneOperatorLoader operator_loader;  
  std::map<operator_id_t, boost::weak_ptr<jetstream::COperator> > operators;
  std::map<operator_id_t, boost::shared_ptr<jetstream::OperatorChain> > sourcelessChain;
  std::map<operator_id_t, boost::shared_ptr<jetstream::OperatorChain> > chainSources;



  void controller_connected (boost::shared_ptr<ClientConnection> conn,
                             boost::system::error_code error);

  void received_ctrl_msg (boost::shared_ptr<ClientConnection> c,
                          const jetstream::ControlMessage &msg,
                          const boost::system::error_code &error);

  void received_data_msg (boost::shared_ptr<ClientConnection> c,
                          const jetstream::DataplaneMessage &msg,
                          const boost::system::error_code &error);
         
  void incoming_conn_handler(boost::shared_ptr<ConnectedSocket> sock,
                             const boost::system::error_code &);


  void make_stop_comput_response (ControlMessage& response,
                                  std::vector<int32_t> stopped_operators,
                                  int32_t compID);

  std::string make_op_list();
  
  void establish_congest_policies( const AlterTopo & topo,
                                   ControlMessage & resp,
                                   const std::vector<operator_id_t>& toStart,
                                   std::map<operator_id_t, boost::shared_ptr<OperatorChain> > &)
                                      throw(operator_err_t);

  void create_chains ( const AlterTopo & topo,
                       ControlMessage & resp,
                       const std::vector<operator_id_t>& toStart,
                       std::vector<OperatorChain* > &,
                       std::map<operator_id_t, boost::shared_ptr<OperatorChain> > &)
                          throw(operator_err_t);

  void log_statistics();
  
 public:
  Node (const NodeConfig &conf, boost::system::error_code &error);
  ~Node ();

  void start (); //starts and returns after creating threads.
  void stop ();
  
  void join ()  {
    boost::unique_lock<boost::mutex> lock(threadpoolLock);
    startStopCond.wait(lock);
  }

  boost::shared_ptr<COperator> get_operator (operator_id_t name);
  
  shared_ptr<COperator>
    create_operator (std::string op_typename, operator_id_t, operator_config_t) throw(operator_err_t);

  bool unregister_chain (boost::shared_ptr<OperatorChain>);
  
  shared_ptr<OperatorChain>
  clone_chain_from( std::map<operator_id_t, boost::shared_ptr<jetstream::OperatorChain> >& chainMap,
                    operator_id_t dest_operator_id);


    //Helper methods for handling incoming commands
  void handle_alter (const AlterTopo& t, ControlMessage& response);
  
  std::vector<int32_t> stop_computation(int32_t compID);
  
  
  unsigned int operator_count () const { 
    boost::shared_lock<boost::shared_mutex> lock(operatorTableLock);
    return operators.size();
  }
  
  const NodeConfig& cfg() { return config;}
    
  boost::shared_ptr<DataCube>
    get_cube (const std::string &name) { return cubeMgr.get_cube(name); }

  const boost::asio::ip::tcp::endpoint & get_listening_endpoint () const
  { return listeningSock->get_local_endpoint(); }


  boost::shared_ptr<boost::asio::deadline_timer> get_timer() {
    return boost::shared_ptr<boost::asio::deadline_timer>(new boost::asio::deadline_timer(*iosrv));
  }
  
  boost::shared_ptr<boost::asio::strand> get_new_strand() {
    return boost::shared_ptr<boost::asio::strand>(new boost::asio::strand(*iosrv));
  }

  Counter bytes_in, bytes_out;


//TODO include private copy-constructor and operator= here?

private:
//  void purgeChains(int compID, std::map<operator_id_t,
//                 shared_ptr<OperatorChain> >& chainMap,
 //                std::vector<int32_t> stopped_ops);

};
}



#endif /* _node_H_ */
