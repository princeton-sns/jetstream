#include <stdlib.h>
#include <boost/thread/thread.hpp>
#include <boost/thread/mutex.hpp>
#include <boost/date_time.hpp>
#include <glog/logging.h>

#include "node.h"
#include "dataplane_comm.h"
#include "jetstream_types.pb.h"
#include "subscriber.h"

using namespace jetstream;
using namespace std;
using namespace boost;


Node::Node (const NodeConfig &conf, boost::system::error_code &error)
  : config (conf),
    iosrv (new asio::io_service()),
    connMgr (new ConnectionManager(iosrv)),
    livenessMgr (iosrv, config),
    webInterface (conf.dataplane_ep.first, conf.webinterface_port, *this),
    cubeMgr(conf),
    dataConnMgr(*iosrv, config),
    operator_cleanup(*iosrv),
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
  asio::ip::address localAddr = asio::ip::address::from_string(config.dataplane_ep.first, error);
  if (error) {
    LOG(FATAL) << "Local worker address " << config.dataplane_ep.first << " is invalid, aborting" << endl;
  }

  asio::ip::tcp::endpoint localEp (localAddr, 
				   config.dataplane_ep.second);

  listeningSock = shared_ptr<ServerConnection>
    (new ServerConnection(iosrv, localEp, error));
  if (error) {
    LOG(ERROR) << "Error creating server socket: " << error.message() << endl;
    return;
  }
  // Update the listener port in the config in case the user specified 0. The config
  // is used by other modules, e.g. the liveness manager.
  config.dataplane_ep.second = listeningSock->get_local_endpoint().port();

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
  // Use created threads as a marker for an already-started node
  if(threads.size() > 0) {
    LOG(WARNING) << "duplicate Node::start; suppressing";
  }

  LOG(INFO) << "starting thread pool with " <<config.thread_pool_size << " threads";
  for (u_int i=0; i < config.thread_pool_size; i++) {
    shared_ptr<thread> t (new thread(bind(&asio::io_service::run, iosrv)));
    threads.push_back(t);
  }
  
  webInterface.start();

//  VLOG(1) << "Finished node::run" << endl;
//  LOG(INFO) << "Finished node::run" << endl;
}


void
Node::stop ()
{
  unique_lock<boost::mutex> lock(threadpoolLock);
 
  // Use the io service status as a marker for an already-stopped node
  if (iosrv->stopped()) {
    VLOG(1) << "Node was stopped twice; suppressing";
    return;
  }

  livenessMgr.stop_all_notifications();
  dataConnMgr.close();
  
  std::map<operator_id_t, shared_ptr<DataPlaneOperator> >::iterator iter = operators.begin();

  // Need to stop operators before deconstructing because otherwise they may
  // keep pointers around after destruction.
  LOG(INFO) << "killing " << operators.size() << " operators on stop";
  while (iter != operators.end()) {
    LOG(INFO) << " stopping " << iter->first << " (" << iter->second->typename_as_str() << ")";
    shared_ptr<DataPlaneOperator> op = iter->second;
    iter++;
    op->stop(); //note that stop will sometimes remove the operator from the table so we advance iterator first;
  }
  
  iosrv->stop();
  LOG(INFO) << "io service stopped: "<< iosrv.get() << endl;
  
  // Optional:  Delete all global objects allocated by libprotobuf.
  // Probably unwise here since we may have multiple Nodes in a unit test.
  //  google::protobuf::ShutdownProtobufLibrary();

  // Wait for all threads in pool to exit; this only happens after the io service
  // is stopped.
  while (threads.size() > 0 ) {
    threads.back()->join();
    threads.pop_back();
    LOG(INFO) << "joined thread " << threads.size();
  }

  operators.empty(); //remove pointers, AFTER the threads stop.

  // Close liveness connections AFTER joining all io service threads, since this
  // guarantees no thread will try to send a notification.
  for (u_int i = 0; i < controllers.size(); ++i) {
    controllers[i]->close_async(no_op_v);
  }
  
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
  
  if (error) {
  
    if (error.value() != boost::system::errc::connection_reset)
      LOG(WARNING) << "Monitoring connection failed: " << error.message();
    return;
  }
  
  
  boost::system::error_code send_error;
  ControlMessage response;

  switch (msg.type ()) {
  case ControlMessage::ALTER:
    {
      const AlterTopo &alter = msg.alter();
      handle_alter(alter, response);
      break;
    }
  case ControlMessage::STOP_COMPUTATION:
    {
      int32_t compID = msg.comp_to_stop();
      vector<int32_t> stopped_operators = stop_computation(compID);
      make_stop_comput_response(response, stopped_operators, compID);

      break;
    }
   default:
     LOG(WARNING) << "Unexpected control message: " << msg.Utf8DebugString() << endl;
     break;
  }

  conn->send_msg(response, send_error);

  if (send_error != boost::system::errc::success) {
    LOG(WARNING) << "Failure sending response: " << send_error.message() << endl;
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
  LOG(INFO) << "Incoming dataplane connection from " << sock->get_remote_endpoint();
  
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
      operator_id_t srcOpID = operator_id_t(e.computation(), e.src());
      //TODO can sanity-check that e is for us here.
      std::string dest_as_str;
      shared_ptr<TupleReceiver> dest;
      if (e.has_dest()) {
        operator_id_t dest_operator_id (e.computation(), e.dest());
        dest_as_str = dest_operator_id.to_string();
        dest = get_operator(dest_operator_id);
      } else if (e.has_dest_cube()) {
        dest_as_str = e.dest_cube();
        dest = cubeMgr.get_cube(dest_as_str);
      }  else {
        LOG(WARNING) << "Got remote chain connect without a dest operator or cube";
        DataplaneMessage response;
        response.set_type(DataplaneMessage::ERROR);
        response.mutable_error_msg()->set_msg("got connect with no dest");
        c->send_msg(response, send_error);
      }

// Operator exists so we can report "ready"
      if (dest) { 
        // Note that it's important to put the connection into receive mode
        // before sending the READY.
        LOG(INFO) << "Chain-connect request for " << dest_as_str << " from " << c->get_remote_endpoint();
       
        dataConnMgr.enable_connection(c, dest, srcOpID);

        //TODO do we log the error or ignore it?
      }
      else {
        LOG(INFO) << "Chain request for " << dest_as_str<< " that isn't ready yet";
        
        dataConnMgr.pending_connection(c, dest_as_str, srcOpID);
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
Node::handle_alter (const AlterTopo& topo, ControlMessage& response)
{
  // Create a response indicating which operators and cubes were successfully
  // started/stopped
  response.set_type(ControlMessage::ALTER_RESPONSE);
  AlterTopo *respTopo = response.mutable_alter();
  respTopo->set_computationid(topo.computationid());

    //a whole handle_alter is atomic with respect to incoming connections
  unique_lock<boost::recursive_mutex> lock(operatorTableLock);


  VLOG(1) << "Incoming Alter message: "<<topo.Utf8DebugString() << endl;

  LOG(INFO) << "Request to create " << topo.tocreate_size() << " cubes and "
      << topo.tostart_size() << " operators." <<endl;


  vector<operator_id_t > operators_to_start;
  for (int i=0; i < topo.tostart_size(); ++i) {
    const TaskMeta &task = topo.tostart(i);
    operator_id_t id = unparse_id(task.id());
    const string &cmd = task.op_typename();
    map<string,string> config;
    for (int j=0; j < task.config_size(); ++j) {
      const TaskMeta_DictEntry &cfg_param = task.config(j);
      config[cfg_param.opt_name()] = cfg_param.val();
    }    
    VLOG(1) << "calling create operator " << id;

    operator_err_t err = create_operator(cmd, id, config);
    VLOG(1) << "create returned " << err;

    if (err == NO_ERR) {
      LOG(INFO) << "configured operator " << id << " of type " << cmd << " ok";

          // Record the outcome of creating the operator in the response message
      TaskMeta *started_task = respTopo->add_tostart();
      started_task->mutable_id()->CopyFrom(task.id());
      started_task->set_op_typename(task.op_typename());
      operators_to_start.push_back(id);    
    }
    else {
      LOG(WARNING) << "aborting creation of " << id << ": " + err;
      respTopo->add_tasktostop()->CopyFrom(task.id());
      
      //teardown started operators
      for (size_t j=0; j < operators_to_start.size(); ++j) {
        //note we don't call stop(), since operators didn't start()
        operators.erase(operators_to_start.at(j));
      }
      response.set_type(ControlMessage::ERROR);
      Error * err_msg = response.mutable_error_msg();
      err_msg->set_msg(err);
      return response;
    }
  }
  
  
  VLOG(1) << "before starting creating cubes, have " << operators.size() << " operators: \n" << make_op_list();
  
  // Create cubes
  for (int i=0; i < topo.tocreate_size(); ++i) {
    const CubeMeta &task = topo.tocreate(i);
    // Record the outcome of creating the cube in the response message
    if (cubeMgr.create_cube(task.name(), task.schema(), task.overwrite_old()) != NULL) {
      LOG(INFO) << "Created cube " << task.name() <<endl;
      assert (cubeMgr.get_cube(task.name()));
      CubeMeta *created = respTopo->add_tocreate();
      created->CopyFrom(task);
    } else {
      LOG(WARNING) << "Failed to create cube " << task.name() <<endl;
        //TODO could tear down operators_to_start here.
      respTopo->add_cubestostop(task.name());
    }
  }
  VLOG(1) << "before starting operators, have " << operators.size() << " operators: \n" << make_op_list();

  // Stop operators if requested
  for (int i=0; i < topo.tasktostop_size(); ++i) {
    operator_id_t id = unparse_id(topo.tasktostop(i));
    LOG(INFO) << "Stopping " << id << " due to server request";
    stop_operator(id);
    // TODO: Should we log whether the stop happened?
    respTopo->add_tasktostop()->CopyFrom(topo.tasktostop(i));
  }
  
  // Remove cubes if requested
  for (int i=0; i < topo.cubestostop_size(); ++i) {
    string id = topo.cubestostop(i);
    LOG(INFO) << "Destroying cube " << id << " due to server request";
    cubeMgr.destroy_cube(id); //Note that this disconnects the cube and marks it
        // as locked. It doesn't actually delete the cube in memory.
        
    // TODO: Should we log whether the stop happened correctly?
  }

  
  // add edges
  for (int i=0; i < topo.edges_size(); ++i) {
    const Edge &edge = topo.edges(i);
    
    if (edge.has_src()) {
      operator_id_t src (edge.computation(), edge.src());
      shared_ptr<DataPlaneOperator> srcOperator = get_operator(src);
    
      if (!srcOperator) {
        LOG(WARNING) << "unknown source operator " << src<< " for edge";
        
        Error * err_msg = response.mutable_error_msg();
        err_msg->set_msg("unknown source operator " + src.to_string());

        continue;
      }
      
      if (edge.has_dest_addr()) {   // sending to remote operator or cube
        shared_ptr<RemoteDestAdaptor> xceiver(
            new RemoteDestAdaptor(dataConnMgr, *connMgr, *iosrv, edge, config.data_conn_wait, srcOperator) );
        dataConnMgr.register_new_adaptor(xceiver);
        srcOperator->set_dest(xceiver);
      }
      else if (edge.has_dest_cube()) {  //local cube
        // connect to local table
        shared_ptr<DataCube> c = cubeMgr.get_cube(edge.dest_cube());
        if (c) {
          srcOperator->set_dest(c);
          LOG(INFO) << "edge from " << src << " to " << edge.dest_cube();
        } else {
          LOG(WARNING) << "Dest DataCube unknown: " << edge.dest_cube() << endl;
          Error * err_msg = response.mutable_error_msg();
          err_msg->set_msg("unknown dest DataCube " + edge.dest_cube());
        }
      }
      else {  // local operator
        assert(edge.has_dest());
        operator_id_t dest (edge.computation(), edge.dest());
        shared_ptr<DataPlaneOperator> destOperator = get_operator(dest);
        if (destOperator) {
          srcOperator->set_dest(destOperator);
          destOperator->add_pred(srcOperator);
          LOG(INFO) << "Edge from " << src << " to " << dest;
         } else {
          LOG(WARNING) << "Edge from " << src<< " to unknown dest " << dest;
          Error * err_msg = response.mutable_error_msg();
          err_msg->set_msg("unknown dest operator " + dest.to_string());
        }
      }
    }
    else { //edge starts at cube. Must go to a subscriber operator in this case.
      shared_ptr<DataCube> c = cubeMgr.get_cube(edge.src_cube());
      operator_id_t dest (edge.computation(), edge.dest());
      shared_ptr<DataPlaneOperator> destOperator = get_operator(dest);
      
      if (!c) {
        LOG(WARNING) << "Can't add edge from " << edge.src_cube() << " to " 
            << edge.dest() << ": no such cube";
      } else if (!destOperator) {
        LOG(WARNING) << "Can't add edge from " << edge.src_cube() << " to " 
            << edge.dest() << ": no such destination";      
      } else {
        shared_ptr<cube::Subscriber> subsc = boost::static_pointer_cast<cube::Subscriber>(destOperator);
        c->add_subscriber(subsc);
      }
    }
  }
  
  // Now start the operators
  //TODO: Should start() return an error? If so, update respTopo.
  vector<operator_id_t >::iterator iter;
  for (iter = operators_to_start.begin(); iter != operators_to_start.end(); iter++) {
    const operator_id_t& name = *iter;
    shared_ptr<DataPlaneOperator> op = get_operator(name);
    assert (op);
    op->start();
    dataConnMgr.created_operator(op);
  }
  
  for (int i=0; i < topo.tocreate_size(); ++i) {
    const CubeMeta &task = topo.tocreate(i);
    shared_ptr<DataCube> c = cubeMgr.get_cube(task.name());
    assert (c);
    dataConnMgr.created_operator(c);
  }
  return response;
}

void
Node::make_stop_comput_response(ControlMessage& response, std::vector<int32_t> stopped_operators, int32_t compID) {
  response.set_type(ControlMessage::ALTER_RESPONSE);
  AlterTopo *respTopo = response.mutable_alter();
  respTopo->set_computationid(compID);
  for (unsigned int i = 0; i < stopped_operators.size(); ++i) {
    TaskID * tID = respTopo->add_tasktostop();
    tID->set_computationid(compID);
    tID->set_task(stopped_operators[i]);
  }
}


vector<int32_t>
Node::stop_computation(int32_t compID) {
  LOG(INFO) << "stopping computation " << compID;
  vector<int32_t> stopped_ops;
  unique_lock<boost::recursive_mutex> lock(operatorTableLock);
  
  std::map<operator_id_t, shared_ptr<DataPlaneOperator> >::iterator iter;
  for ( iter = operators.begin(); iter != operators.end(); ) {
    operator_id_t op_id = iter->first;
    boost::shared_ptr<DataPlaneOperator>  op = iter->second;
     //need to advance iterator BEFORE stop, since iterator to removed element is invalid
    iter ++;
    
    if (op_id.computation_id == compID) {
    
      stopped_ops.push_back(op_id.task_id);
      // The actual stop. 
      operator_cleanup.stop_on_strand(op);
      operators.erase(op_id);
      operator_cleanup.cleanup(op);      
    }
  }
  LOG(INFO) << "stopped " << stopped_ops.size() << " operators for computation " <<
    compID << " , leaving " << operators.size()<<":" << make_op_list();
  return stopped_ops ;
}

boost::shared_ptr<DataPlaneOperator> 
Node::get_operator (operator_id_t name) {
  unique_lock<boost::recursive_mutex> lock(operatorTableLock);
  
  std::map<operator_id_t, shared_ptr<DataPlaneOperator> >::iterator iter;
  iter = operators.find(name);
  if (iter != operators.end())
    return iter->second;
  else {
    boost::shared_ptr<DataPlaneOperator> x;
    return x; 
  }
}

operator_err_t
Node::create_operator (string op_typename, operator_id_t name, map<string,string> cfg)
{
  shared_ptr<DataPlaneOperator> d (operator_loader.newOp(op_typename));
  if (d == NULL) {
    LOG(WARNING) <<" failed to create operator object. Type was "<<op_typename <<endl;
    return operator_err_t("Loader failed to create operator of type " + op_typename);
  }
  
  d->id() = name;
  d->set_node(this);
  VLOG(1) << "configuring " << name << " of type " << op_typename;
  operator_err_t err = d->configure(cfg);
  if (err == NO_ERR) {
    operators[name] = d; //TODO check for name in use?
  }
  return err;
}

bool 
Node::stop_operator(operator_id_t name) {
// No need for locking provided that operator is invoked in the control
//  connection strand. Otherwise would need to avoid concurrent modification
//  to the operator table

  std::map<operator_id_t, shared_ptr<DataPlaneOperator> >::iterator iter;
  iter = operators.find(name);
  if (iter != operators.end())  { //operator still around
    shared_ptr<DataPlaneOperator> op = iter->second;
    operator_cleanup.stop_on_strand(op);
    
    {
      unique_lock<boost::recursive_mutex> lock(operatorTableLock);
      int delCount = operators.erase(name);
      LOG_IF(FATAL, delCount == 0) << "Couldn't find a " << name << " to erase from operators table";
    }
    
    operator_cleanup.cleanup(op);
  
    return true;
// TODO: should unload code at some point. Presumably when no more operators
// of that type are running? Can we push that into operatorloader?

  }
  return false;
}



std::string
Node::make_op_list() {
  unique_lock<boost::recursive_mutex> lock(operatorTableLock);

  ostringstream s;
  std::map<operator_id_t, shared_ptr<DataPlaneOperator> >::iterator iter;
  for ( iter = operators.begin(); iter != operators.end(); ++iter) {
    operator_id_t op_id = iter->first;
    boost::shared_ptr<DataPlaneOperator>  op = iter->second;
    if (op)
      s << "\t" << op_id << " " << op->typename_as_str() << endl;
    else
      s << "\t" << op_id << " NULL" << endl;

  }
  return s.str();
}

