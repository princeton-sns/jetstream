#include <boost/thread/thread.hpp>
#include <boost/thread/mutex.hpp>
#include <boost/date_time.hpp>

#include "node.h"

#include "jetstream_types.pb.h"

#include <stdlib.h>
#include <glog/logging.h>


using namespace jetstream;
using namespace std;
using namespace boost;

mutex _node_mutex;

Node::Node (const NodeConfig &conf)
  : config (conf),
    iosrv (new asio::io_service()),
    conn_mgr (new ConnectionManager(iosrv)),
    liveness_mgr (new LivenessManager (iosrv, conf.heartbeat_time)),
    // XXX This should get set through config files
    operator_loader ("src/dataplane/") //NOTE: path must end in a slash
{
  LOG(INFO) << "creating node" << endl;
  // Set up the network connection
  asio::io_service::work work(*iosrv);

  if (conf.heartbeat_time > 0) {
    // XXX Can't dynamically modify heartbeat_time. Pass pointer to config?
    for (u_int i=0; i < config.controllers.size(); i++) {
      LOG(INFO) << "possible controller: " << config.controllers[i].first <<
          ":"<<config.controllers[i].second <<endl;
      pair<string, port_t> cntrl = config.controllers[i];
      conn_mgr->create_connection (cntrl.first, cntrl.second,
				   bind(&Node::controller_connected, 
					this, _1, _2));
    }
  }
}


Node::~Node () 
{
}


/***
*  Run the node. This method starts several threads and does not return until
* the node process is terminated.
*/
void
Node::run ()
{
  for (u_int i=0; i < config.thread_pool_size; i++) {
    shared_ptr<thread> t (new thread(bind(&asio::io_service::run, iosrv)));
    threads.push_back(t);
  }

  // Wait for all threads in pool to exit
  for (u_int i=0; i < threads.size(); i++)
    threads[i]->join();

  iosrv->run ();

  LOG(INFO) << "Finished node::run" << endl;
}


void
Node::stop ()
{
  liveness_mgr->stop_all_notifications();
  iosrv->stop();
  LOG(INFO) << "io service stopped" << endl;

  // Optional:  Delete all global objects allocated by libprotobuf.
  google::protobuf::ShutdownProtobufLibrary();
  LOG(INFO) << "Finished node::stop" << endl;
}


void
Node::controller_connected (shared_ptr<ClientConnection> conn,
			    boost::system::error_code error)
{
  if (error) {
    _node_mutex.lock();
    LOG(WARNING) << "Node: Monitoring connection failed: " << error.message() << endl;
    _node_mutex.unlock();
    return;
  }

  controllers.push_back(conn);

  if (!liveness_mgr) {
    _node_mutex.lock();
    LOG(WARNING) << "Node: Liveness manager NULL" << endl;
    _node_mutex.unlock();
  }
  else {
    _node_mutex.lock();
    LOG(INFO) << "Node: Connected to controller: "
<< conn->get_remote_endpoint() << endl;
    _node_mutex.unlock();
    liveness_mgr->start_notifications(conn);
  }

  // Start listening on messages from controller
  boost::system::error_code e;


  conn->recv_ctrl_msg(bind(&Node::received_ctrl_msg, this, conn,  _1, _2), e);
}


void
Node::received_ctrl_msg (shared_ptr<ClientConnection> c, const jetstream::ControlMessage &msg,
		    const boost::system::error_code &error)
{
//  VLOG(1) << "got message: " << msg.Utf8DebugString() <<endl;
  
  boost::system::error_code send_error;
  switch (msg.type ()) {
  case ControlMessage::ALTER:
    {
      const AlterTopo &alter = msg.alter();
      ControlMessage response = handle_alter(alter);
      c->send_msg(response, send_error);

      if (send_error != boost::system::errc::success) {
        LOG(WARNING) << "failure sending response: "<<send_error <<endl;
      }
        
      break;
    }
/*  case DATAPLANE:
    {
      const DataMsg &data_msg = msg.get_DataPlane();
      if (!data_msg)
	error == X;
      else 
	received_dataplane_msg(data_msg, error);
      break;
    }*/
   default:
     break;
  }
  
}


#if 0
void
Node::received_data_msg (const DataplaneMessage &msg,
				 const system::error_code &error)
{
  switch (msg.getType ()) {
  case DATA:
    {
      handle_alter (msg.get_alter());
      break;
    }
  default:
  }
}
#endif



operator_id_t 
unparse_id (const TaskID& id)
{
  operator_id_t parsed;
  parsed.computation_id = id.computationid();
  parsed.task_id = id.task();
  return parsed;
}


ControlMessage
Node::handle_alter (const AlterTopo& topo)
{
  map<operator_id_t, map<string, string> > operator_configs;
  for (int i=0; i < topo.tostart_size(); ++i) {
    const TaskMeta &task = topo.tostart(i);
    operator_id_t id = unparse_id(task.id());
    const string &cmd = task.op_typename();
    map<string,string> config;
    for (int j=0; j < task.config_size(); ++j) {
      const TaskMeta_DictEntry &cfg_param = task.config(j);
      config[cfg_param.opt_name()] = cfg_param.val();
    }
    operator_configs[id] = config;
    create_operator(cmd, id);
     //TODO: what if this returns a null pointer, indicating create failed?
  }
  
  //make cubes here
  for (int i=0; i < topo.tocreate_size(); ++i) {
    const CubeMeta &task = topo.tocreate(i);
    cube_mgr.create_cube(task.name(), task.schema());
  }
  
  //TODO remove cubes and operators if specified.
  
  
  
  
  // add edges
  for (int i=0; i < topo.edges_size(); ++i) {
    const Edge& e = topo.edges(i);
    operator_id_t src( e.computation(), e.src());
    shared_ptr<DataPlaneOperator> src_op = get_operator(src);
    
    if (e.has_cube_name()) {     //connect to local table
      shared_ptr<DataCube> d = cube_mgr.get_cube(e.cube_name());
      src_op->set_dest(d);
    } 
    else if (e.has_dest_addr()) {   //remote network operator
      //TODO handle network
    } 
    else {
      assert(e.has_dest());
      operator_id_t dest( e.computation(), e.dest());
      shared_ptr<DataPlaneOperator> dest_op = get_operator(dest);
      src_op->set_dest(dest_op);      
    }
  }
  
    //now start the operators
  map<operator_id_t, map<string,string> >::iterator iter;
  for (iter = operator_configs.begin(); iter != operator_configs.end(); iter++) {
    shared_ptr<DataPlaneOperator> o = get_operator(iter->first);
    o->start(iter->second);
  }
  
  ControlMessage m;
  m.set_type(ControlMessage::OK);
  return m;
}


shared_ptr<DataPlaneOperator>
Node::create_operator(string op_typename, operator_id_t name) 
{
  shared_ptr<DataPlaneOperator> d (operator_loader.newOp(op_typename));

   //TODO logging
/*
  if (d.get() != NULL) {
    cout << "creating operator of type " << op_typename <<endl;
  else 
  {
    cout <<" failed to create operator. Type was "<<op_typename <<endl;
  } */
//  d->operID = name.task_id;
  operators[name] = d;
  return d;
}


