#include <stdlib.h>
#include <boost/thread/thread.hpp>
#include <boost/thread/mutex.hpp>
#include <boost/date_time.hpp>
#include <glog/logging.h>

#include "node.h"
#include "dataplane_comm.h"
#include "jetstream_types.pb.h"


using namespace jetstream;
using namespace std;
using namespace boost;

Node::Node (const NodeConfig &conf, boost::system::error_code &error)
  : config (conf),
    iosrv (new asio::io_service()),
    connMgr (new ConnectionManager(iosrv)),
    livenessMgr (iosrv, conf.heartbeat_time),
    webInterface (conf.webinterface_port, *this),

    // TODO This should get set through config files
    operator_loader ("src/dataplane/") //NOTE: path must end in a slash
{
  LOG(INFO) << "creating node" << endl;

  // Set up the network connection
  asio::io_service::work work(*iosrv);

  if (conf.heartbeat_time > 0) {
    // XXX Can't dynamically modify heartbeat_time. Pass pointer to config?
    for (u_int i=0; i < config.controllers.size(); i++) {

      LOG(INFO) << "possible controller: " << config.controllers[i].first 
		<< ":" << config.controllers[i].second << endl;

      pair<string, port_t> cntrl = config.controllers[i];
      connMgr->create_connection(cntrl.first, cntrl.second,
				 bind(&Node::controller_connected, 
				      this, _1, _2));
    }
  }

  // Setup incoming connection listener
  asio::ip::tcp::endpoint listen_port (asio::ip::tcp::v4(), 
				       config.dataplane_myport);

  listeningSock = shared_ptr<ServerConnection>
    (new ServerConnection(iosrv, listen_port, error));

  if (error) {
    LOG(ERROR) << "Error creating server socket: " << error.message() << endl;
    return;
  }

  listeningSock->accept(bind(&Node::incoming_conn_handler, this, _1, _2), error);

  if (error) {
    LOG(ERROR) << "Error accepting server socket: " << error.message() << endl;
    return;
  }
}


Node::~Node () 
{
  stop();
}


/***
* Run the node. This method starts several threads and does not return until
* the node process is terminated.
*/
void
Node::start ()
{

  LOG(INFO) << "starting thread pool with " <<config.thread_pool_size << " threads";
  for (u_int i=0; i < config.thread_pool_size; i++) {
    shared_ptr<thread> t (new thread(bind(&asio::io_service::run, iosrv)));
    threads.push_back(t);
  }
  
  webInterface.start();

//  iosrv->run();

//  VLOG(1) << "Finished node::run" << endl;
//  LOG(INFO) << "Finished node::run" << endl;
}


void
Node::stop ()
{
  unique_lock<boost::mutex> lock(threadpoolLock);
  
  if (iosrv->stopped()) {  //use the io serv as a marker for already-stopped
    VLOG(1) << "Node was stopped twice; suppressing";
    return;
  }

  

  livenessMgr.stop_all_notifications();
  dataConnMgr.close();
  for (u_int i = 0; i < controllers.size(); ++i) {
    controllers[i]->close();
  }
  
  iosrv->stop();
  LOG(INFO) << "io service stopped" << endl;
  
  
  std::map<operator_id_t, shared_ptr<DataPlaneOperator> >::iterator iter;

  // Need to stop operators before deconstructing because otherwise they may
  // keep pointers around after destruction.
  LOG(INFO) << "killing " << operators.size() << " operators on stop";
  for (iter = operators.begin(); iter != operators.end(); iter++) {
    iter->second->stop();
  }
  
  // Optional:  Delete all global objects allocated by libprotobuf.
  // Probably unwise here since we may have multiple Nodes in a unit test.
  //  google::protobuf::ShutdownProtobufLibrary();

  // Wait for all threads in pool to exit; this only happens when the io service
  //  is stopped.
  while (threads.size() > 0 ) {
    threads.back()->join();
    threads.pop_back();
    LOG(INFO) << "joined thread " << threads.size();
  }

  operators.empty(); //remove pointers, AFTER the threads stop.
  
  startStopCond.notify_all();

  LOG(INFO) << "Finished node::stop" << endl;
}


void
Node::controller_connected (shared_ptr<ClientConnection> conn,
			    boost::system::error_code error)
{
  if (error) {
    LOG(WARNING) << "Monitoring connection failed: " << error.message();
    return;
  }

  controllers.push_back(conn);

  LOG(INFO) << "Connected to controller: " << conn->get_remote_endpoint();
  livenessMgr.start_notifications(conn);

  // Start listening on messages from controller
  boost::system::error_code e;
  conn->recv_control_msg(bind(&Node::received_ctrl_msg, this, conn, _1, _2), e);
}


void
Node::received_ctrl_msg (shared_ptr<ClientConnection> conn, 
			 const jetstream::ControlMessage &msg,
			 const boost::system::error_code &error)
{
 // VLOG(1) << "got message: " << msg.Utf8DebugString() <<endl;
  
  boost::system::error_code send_error;
  switch (msg.type ()) {
  case ControlMessage::ALTER:
    {
      ControlMessage response;
      const AlterTopo &alter = msg.alter();
      handle_alter(response, alter);
      conn->send_msg(response, send_error);

      if (send_error != boost::system::errc::success) {
        LOG(WARNING) << "Failure sending response: " << send_error.message() << endl;
      }
        
      break;
    }

   default:
     break;
  }
  
  // Wait for the next control message 
  boost::system::error_code e;
  conn->recv_control_msg(bind(&Node::received_ctrl_msg, this, conn, _1, _2), e);
}


void
Node::incoming_conn_handler (boost::shared_ptr<ConnectedSocket> sock, 
			     const boost::system::error_code &error)
{
  if (error) {
    if (!iosrv->stopped())
      LOG(WARNING) << "Error receiving incoming connection: " << error.value()
		   << "(" << error.message() << ")" << endl;
    return;
  }
  boost::system::error_code e;
  
  // Need to convert the connected socket to a client_connection
  LOG(INFO) << "Incoming dataplane connection received ok";
  
  boost::shared_ptr<ClientConnection> conn (new ClientConnection(sock));
  conn->recv_data_msg(bind(&Node::received_data_msg, this, conn,  _1, _2), e);  

}


/**
 * This is only invoked for a "new" connection. We change the signal handler
 *  here to the appropriate handler object.
 */
void
Node::received_data_msg (shared_ptr<ClientConnection> c,
			 const DataplaneMessage &msg,
			 const boost::system::error_code &error)
{
  boost::system::error_code send_error;

  VLOG(1) << "Received data message of type " << msg.type();

  switch (msg.type ()) {
  case DataplaneMessage::CHAIN_CONNECT:
    {
      const Edge& e = msg.chain_link();
      //TODO can sanity-check that e is for us here.
      if (e.has_dest()) {
      
        operator_id_t dest_operator_id (e.computation(), e.dest());
        shared_ptr<DataPlaneOperator> dest = get_operator(dest_operator_id);
        
        LOG(INFO) << "Chain request for operator " << dest_operator_id.to_string();
	// Operator exists so we can report "ready"
        if (dest) { 
          // Note that it's important to put the connection into receive mode
          // before sending the READY.
         
          dataConnMgr.enable_connection(c, dest_operator_id, dest);

          //TODO do we log the error or ignore it?
        }
        else {
          LOG(INFO) << "Chain request for operator that isn't ready yet";
          dataConnMgr.pending_connection(c, dest_operator_id);
        }
      }
      else {
        LOG(WARNING) << "Got remote chain connect without a dest";
        DataplaneMessage response;
        response.set_type(DataplaneMessage::ERROR);
        response.mutable_error_msg()->set_msg("got connect with no dest");
        c->send_msg(response, send_error);
      }
    }
    break;
  default:
     // Everything else is an error
     LOG(WARNING) << "Unexpected dataplane message: " << msg.type() << 
        " from " << c->get_remote_endpoint();
        
     break;
  }
}


operator_id_t 
unparse_id (const TaskID& id) {
  operator_id_t parsed;
  parsed.computation_id = id.computationid();
  parsed.task_id = id.task();
  return parsed;
}


ControlMessage
Node::handle_alter (ControlMessage& response, const AlterTopo& topo)
{
  // Create a response indicating which operators and cubes were successfully
  // started/stopped
  response.set_type(ControlMessage::ALTER_RESPONSE);
  AlterTopo *respTopo = response.mutable_alter();
  respTopo->set_computationid(topo.computationid());

  VLOG(1) << topo.Utf8DebugString() << endl;

  LOG(INFO) << "Request to create " << topo.tocreate_size() << " cubes and "
      << topo.tostart_size() << " operators." <<endl;


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
    // Record the outcome of creating the operator in the response message
    if (create_operator(cmd, id) != NULL) {
      TaskMeta *started_task = respTopo->add_tostart();
      started_task->mutable_id()->CopyFrom(task.id());
      started_task->set_op_typename(task.op_typename());
    } else {
      respTopo->add_tasktostop()->CopyFrom(task.id());
    }
  }

  // Create cubes
  for (int i=0; i < topo.tocreate_size(); ++i) {
    const CubeMeta &task = topo.tocreate(i);
    // Record the outcome of creating the cube in the response message
    if (cubeMgr.create_cube(task.name(), task.schema(), task.overwrite_old()) != NULL) {
      LOG(INFO) << "Created cube " << task.name() <<endl;
      CubeMeta *created = respTopo->add_tocreate();
      created->CopyFrom(task);
    } else {
      LOG(WARNING) << "Failed to create cube " << task.name() <<endl;
      respTopo->add_cubestostop(task.name());
    }
  }

  // Stop operators if need be
  for (int i=0; i < topo.tasktostop_size(); ++i) {
    operator_id_t id = unparse_id(topo.tasktostop(i));
    LOG(INFO) << "Stopping " << id << " due to server request";
    stop_operator(id);
    // TODO: Should we log whether the stop happened?
    respTopo->add_tasktostop()->CopyFrom(topo.tasktostop(i));
  }
  
  // Remove cubes if specified.
  for (int i=0; i < topo.cubestostop_size(); ++i) {
    string id = topo.cubestostop(i);
    LOG(INFO) << "Closing cube " << id << " due to server request";
    cubeMgr.destroy_cube(id); //Note that this disconnects the cube and marks it
        // as locked. It doesn't actually delete the cube in memory.
        
    // TODO: Should we log whether the stop happened correctly?
  }

  
  // add edges
  for (int i=0; i < topo.edges_size(); ++i) {
    const Edge &edge = topo.edges(i);
    operator_id_t src (edge.computation(), edge.src());
    shared_ptr<DataPlaneOperator> srcOperator = get_operator(src);
    
    assert(srcOperator);

    //TODO check if src doesn't exist
    if (edge.has_cube_name()) { 
      // connect to local table
      shared_ptr<DataCube> c = cubeMgr.get_cube(edge.cube_name());
      if (c)
        srcOperator->set_dest(c);
      else {
        LOG(WARNING) << "DataCube unknown: " << edge.cube_name() << endl;
      }
    } 
    else if (edge.has_dest_addr()) {   //remote network operator
      shared_ptr<TupleReceiver> xceiver(
          new RemoteDestAdaptor(*connMgr, edge) );
      srcOperator->set_dest(xceiver);
    } 
    else {
      assert(edge.has_dest());
      operator_id_t dest (edge.computation(), edge.dest());
      shared_ptr<DataPlaneOperator> destOperator = get_operator(dest);
      if (destOperator)
        srcOperator->set_dest(destOperator);
      else {
        LOG(WARNING) << "Dest Operator unknown: " << dest.to_string() << endl;
      }
    }
  }
  
  // Now start the operators
  //TODO: Should start() return an error? If so, update respTopo.
  map<operator_id_t, map<string,string> >::iterator iter;
  for (iter = operator_configs.begin(); iter != operator_configs.end(); iter++) {
    shared_ptr<DataPlaneOperator> op = get_operator(iter->first);
    op->start(iter->second);
  }

  return response;
}

boost::shared_ptr<DataPlaneOperator> 
Node::get_operator (operator_id_t name) { 
  std::map<operator_id_t, shared_ptr<DataPlaneOperator> >::iterator iter;
  iter = operators.find(name);
  if (iter != operators.end())
    return iter->second;
  else {
    boost::shared_ptr<DataPlaneOperator> x;
    return x; 
  }
}

shared_ptr<DataPlaneOperator>
Node::create_operator (string op_typename, operator_id_t name) 
{
  shared_ptr<DataPlaneOperator> d (operator_loader.newOp(op_typename));
  d->id() = name;
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
  dataConnMgr.created_operator(name, d);
  return d;
}

bool 
Node::stop_operator(operator_id_t name) {
// No need for locking provided that operator is invoked in the control
//  connection strand. Otherwise would need to avoid concurrent modification
//  to the operator table
  shared_ptr<DataPlaneOperator> op = operators[name];
  if (op) { //operator still around
    op->stop();
    int delCount = operators.erase(name);
    assert(delCount > 0);
    return true;
// TODO: should unload code at some point. Presumably when no more operators
// of that type are running? Can we push that into operatorloader?

  }
  return false;
}
