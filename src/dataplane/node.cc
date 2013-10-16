#include <stdlib.h>
#include <boost/thread/thread.hpp>
#include <boost/thread/mutex.hpp>
#include <boost/date_time.hpp>
#include <glog/logging.h>

#include "node.h"
#include "dataplane_comm.h"
#include "jetstream_types.pb.h"
#include "subscriber.h"
#include <typeinfo>

using namespace jetstream;
//using namespace std;
using std::string;
using std::vector;
using std::endl;
using std::map;
using std::ostringstream;
using std::pair;
using namespace boost;

bool PRINT_STATS = false;

Node::Node (const NodeConfig &conf, boost::system::error_code &error)
  : config (conf),
    iosrv (new asio::io_service()),
    iosrv_work(*iosrv),
    connMgr (new ConnectionManager(iosrv, conf.connection_buffer_size)),
    livenessMgr (iosrv, config),
    webInterface (conf.dataplane_ep.first, conf.webinterface_port, *this),
    cubeMgr(conf),
    dataConnMgr(*iosrv, this),
    // TODO This should get set through config files
    operator_loader ("src/dataplane/") //NOTE: path must end in a slash
{
  LOG(INFO) << "creating node";

  // Set up the network connection
  //asio::io_service::work work(*iosrv);

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
    LOG(FATAL) << "Local worker address " << config.dataplane_ep.first << " is invalid, aborting";
  }

  asio::ip::tcp::endpoint localEp (localAddr, 
				   config.dataplane_ep.second);

  listeningSock = shared_ptr<ServerConnection>
    (new ServerConnection(iosrv, localEp, error));
  if (error) {
    LOG(ERROR) << "Error creating server socket: " << error.message();
    return;
  }
  // Update the listener port in the config in case the user specified 0. The config
  // is used by other modules, e.g. the liveness manager.
  config.dataplane_ep.second = listeningSock->get_local_endpoint().port();
    
  LOG(INFO) << "*********** \n" << "\t\t\t\t\tNode ID is " << listeningSock->get_local_endpoint()<<
        "\n \t\t\t\t\t     *********** ";// <<":" << config.dataplane_ep.second;

  listeningSock->accept(bind(&Node::incoming_conn_handler, this, _1, _2), error);
  if (error) {
    LOG(ERROR) << "Error accepting server socket: " << error.message();
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
  dataConnMgr.set_counters(&bytes_out, &bytes_in);
  webInterface.start();
  if(PRINT_STATS)
    iosrv->post(bind(&Node::log_statistics, this));

//  VLOG(1) << "Finished node::run" << endl;
//  LOG(INFO) << "Finished node::run" << endl;
}


void
Node::stop ()
{
 
  // Use the io service status as a marker for an already-stopped node
  if (iosrv->stopped()) {
    VLOG(1) << "Node was stopped twice; suppressing";
    return;
  }
  
  webInterface.stop();
  livenessMgr.stop_all_notifications();
  dataConnMgr.close();
    
  std::map<operator_id_t, shared_ptr<OperatorChain> >::iterator chainIter = chainSources.begin();
  while (chainIter != chainSources.end()) {
    chainIter->second->stop();
    chainIter ++;
  }
  
  {
    unique_lock<boost::shared_mutex> lock(operatorTableLock);
    
    std::map<operator_id_t, weak_ptr<COperator> >::iterator iter = operators.begin();
    // Need to stop operators before deconstructing because otherwise they may
    // keep pointers around after destruction.
    LOG(INFO) << "killing " << operators.size() << " operators on stop";
    while (iter != operators.end()) {
      shared_ptr<COperator> op = iter->second.lock();
      operator_id_t id = iter->first;
      iter++;//note that stop will sometimes remove the operator from the table so we advance iterator first;
      if (op) {
        LOG(INFO) << " freeing " << id << " (" << op->typename_as_str() << ")";
        op.reset();
      }
    }
  }
  
  iosrv->stop(); //need to do this BEFORE stopping the threads apparently? -asr 6/13/13
  LOG(INFO) << "io service stopped: "<< iosrv.get() << endl;
  
  
  while (threads.size() > 0 ) {
    threads.back()->join();
    threads.pop_back();
    LOG(INFO) << "joined thread " << threads.size();
  }
  
  
  // Optional:  Delete all global objects allocated by libprotobuf.
  // Probably unwise here since we may have multiple Nodes in a unit test.
  //  google::protobuf::ShutdownProtobufLibrary();

  // Wait for all threads in pool to exit; this only happens after the io service
  // is stopped.

  operators.empty(); //remove pointers, AFTER the threads stop.

  // Close liveness connections AFTER joining all io service threads, since this
  // guarantees no thread will try to send a notification.
  for (u_int i = 0; i < controllers.size(); ++i) {
    controllers[i]->close_async(no_op_v);
  }
  
  startStopCond.notify_all();

  LOG(INFO) << "Finished node::stop" << endl;
}

void Node::log_statistics()
{
  while(!iosrv->stopped())
  {
    boost::this_thread::sleep(boost::posix_time::milliseconds(2000));
    LOG(INFO) << "Node Statistics: bytes_in="  << bytes_in.read() << " bytes_out=" <<bytes_out.read() ;
  }
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
  
  unique_lock<boost::mutex> lock(jobStartLock);
  
  boost::system::error_code send_error;
  ControlMessage response;

  switch (msg.type ()) {
  case ControlMessage::ALTER:
    {
      for (int i = 0; i < msg.alter_size(); ++i)
        handle_alter(msg.alter(i), response);
      break;
    }
  case ControlMessage::STOP_COMPUTATION:
    {
      LOG(INFO) << "Got stop-computation message from server";
      int32_t compID = msg.comp_to_stop();
      vector<int32_t> stopped_operators = stop_computation(compID);
      make_stop_comput_response(response, stopped_operators, compID);

      break;
    }
  case ControlMessage::ERROR:
    //this is a special case to prevent a loop.
     LOG(WARNING) << "Unexpected error message on control connection: " << msg.Utf8DebugString() << endl;
     break;
   default:
     LOG(WARNING) << "Unexpected control message: " << msg.Utf8DebugString() << endl;
     response.set_type(ControlMessage::ERROR);
     response.mutable_error_msg()->set_msg("Got unexpected message from controller");
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
  sock->set_counters(&bytes_out, &bytes_in);
  boost::shared_ptr<ClientConnection> conn (new ClientConnection(sock));
  conn->recv_data_msg(bind(&Node::received_data_msg, this, conn,  _1, _2), e);  

}

shared_ptr<OperatorChain> 
Node::clone_chain_from(std::map<operator_id_t, boost::shared_ptr<jetstream::OperatorChain> >& chainMap, operator_id_t dest_operator_id) {
  shared_ptr<OperatorChain> new_chain;
  if (chainMap.count(dest_operator_id) > 0) {
    new_chain = shared_ptr<OperatorChain>(new OperatorChain());
    new_chain->add_member();
    new_chain->clone_from(chainMap[dest_operator_id]);
    for ( unsigned i = 1; i < new_chain->members(); ++i) {
      new_chain->member(i)->add_chain(new_chain);
    }
    LOG(INFO) << "Cloned a chain; now has " << new_chain->members() << " members";
  }
  return new_chain;
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
  unique_lock<boost::mutex> lock(jobStartLock);


  switch (msg.type ()) {
  case DataplaneMessage::CHAIN_CONNECT:
    {
//      LOG(INFO) << "handling chain-connect";
      const Edge& e = msg.chain_link();
      operator_id_t srcOpID = operator_id_t(e.computation(), e.src());
      //TODO can sanity-check that e is for us here.
      std::string dest_as_str;
      shared_ptr<OperatorChain> new_chain;
      if (e.has_dest()) { //already exists
        operator_id_t dest_operator_id (e.computation(), e.dest());
        dest_as_str = dest_operator_id.to_string();
        
        new_chain = clone_chain_from(sourcelessChain, dest_operator_id);
        if (!new_chain)
          new_chain = clone_chain_from(chainSources, dest_operator_id);
      } else if (e.has_dest_cube()) {
        dest_as_str = e.dest_cube();
        boost::shared_ptr<DataCube> dest = cubeMgr.get_cube(dest_as_str);
        if (dest) {
          new_chain = shared_ptr<OperatorChain>(new OperatorChain());
          new_chain->add_member();
          new_chain->add_member(dest);
          dest->add_chain(new_chain);
        }
      }  else {
        LOG(WARNING) << "Got remote chain connect without a dest operator or cube";
        DataplaneMessage response;
        response.set_type(DataplaneMessage::ERROR);
        response.mutable_error_msg()->set_msg("got connect with no dest");
        c->send_msg(response, send_error);
      }

// Operator exists so we can report "ready"
      if (new_chain) { 
        // Note that it's important to put the connection into receive mode
        // before sending the READY.
        LOG(INFO) << "On recipient, chain-connect request for " << dest_as_str << " from " << c->get_remote_endpoint();
       
        dataConnMgr.enable_connection(c, new_chain, srcOpID);

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

void
Node::handle_alter (const AlterTopo& topo, ControlMessage& response) {
  
  // Create a response indicating which operators and cubes were successfully
  // started/stopped
  response.set_type(ControlMessage::ALTER_RESPONSE);
  AlterTopo *respTopo = response.add_alter();
  respTopo->set_computationid(topo.computationid());

    //a whole handle_alter is atomic with respect to incoming connections

  VLOG(1) << "Incoming Alter message: "<<topo.Utf8DebugString();

  LOG(INFO) << "Request to create " << topo.tocreate_size() << " cubes and "
      << topo.tostart_size() << " operators.";
  vector<operator_id_t > operator_ids_to_start;

  try {

    vector<shared_ptr<COperator> > operators_to_start;
    {
      unique_lock<boost::shared_mutex> lock(operatorTableLock);

      for (int i=0; i < topo.tostart_size(); ++i) {
        const TaskMeta &task = topo.tostart(i);
        operator_id_t id = unparse_id(task.id());
        const string &cmd = task.op_typename();

        
        if ( operators.count(id) > 0) {
          vector <operator_id_t>::iterator objPos = find (operator_ids_to_start.begin(),
                                                          operator_ids_to_start.end(), id);
          if (objPos != operator_ids_to_start.end())
            throw operator_err_t("Operator " + id.to_string() + " already defined in this alter");
          boost::shared_ptr<COperator> prev_op = operators[id].lock();
          const string& existing_typename = typeid(*prev_op).name();
          if (existing_typename.find(cmd) == string::npos)
            throw operator_err_t("operator " + id.to_string() + " clashes with " + prev_op->long_description());
          else
            LOG(INFO) << "Already got an operator " << id << "of type " << existing_typename;
        } else {
          map<string,string> config;
          for (int j=0; j < task.config_size(); ++j) {
            const TaskMeta_DictEntry &cfg_param = task.config(j);
            config[cfg_param.opt_name()] = cfg_param.val();
          }    
          VLOG(1) << "calling create operator " << id;

          shared_ptr<COperator> op = create_operator(cmd, id, config);
          operators_to_start.push_back( op);
          operator_ids_to_start.push_back(id);
          LOG(INFO) << "configured operator " << id << " of type " << cmd << " ok";
          operators[id] = op;
        }
        TaskMeta *started_task = respTopo->add_tostart();
        started_task->mutable_id()->CopyFrom(task.id());
        started_task->set_op_typename(task.op_typename());
      }
    }

  //Invariant: if we got here, all operators started OK.
  
    VLOG(1) << "before creating cubes, have " << operators.size() << " operators: \n" << make_op_list();
    
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
        throw operator_err_t("Failed to create cube " + task.name());
      }
    }
    VLOG(1) << "after creating cubes, have " << operators.size() << " operators: \n" << make_op_list();

    // Stop operators if requested
    for (int i=0; i < topo.tasktostop_size(); ++i) {
      operator_id_t id = unparse_id(topo.tasktostop(i));
      LOG(INFO) << "Stopping " << id << " due to server request";
      shared_ptr<OperatorChain> c = chainSources[id];
      if (c) {
        c->stop();
        unregister_chain(c);
      }
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

    vector< OperatorChain* > newChains;
    map<operator_id_t, boost::shared_ptr<OperatorChain> > opToChain;
    create_chains(topo, response, operator_ids_to_start, newChains, opToChain);
    establish_congest_policies(topo, response, operator_ids_to_start, opToChain);
    
  // Now start the operators
  //TODO: Should start() return an error? If so, update respTopo.

    vector<OperatorChain* >::iterator iter;
    for (iter = newChains.begin(); iter != newChains.end(); iter++) {
      OperatorChain * chain = *iter;
      chain->start();
    }
  
    for (int i=0; i < topo.tocreate_size(); ++i) {
      const CubeMeta &task = topo.tocreate(i);
      shared_ptr<DataCube> c = cubeMgr.get_cube(task.name());
      assert (c);
      shared_ptr<OperatorChain> chain(new OperatorChain); //for incoming connections to cubes
      chain->add_member(c);
      dataConnMgr.created_chain(chain);
    }
    
    
  } catch(operator_err_t err) {
    LOG(WARNING) << "aborting alter: " + err;

    vector<operator_id_t >::iterator iter;
    for (iter = operator_ids_to_start.begin(); iter != operator_ids_to_start.end(); iter++) {
      operator_id_t name = *iter;
        //note that we aren't iterating over any of these, but over a separate vector
      shared_ptr<COperator> to_destroy = get_operator(name); //don't destruct until tables are modified.
      sourcelessChain.erase(name);
      chainSources.erase(name);
      operators.erase(name);
      //destructor happens here
    }
    response.set_type(ControlMessage::ERROR);
    Error * err_msg = response.mutable_error_msg();
    err_msg->set_msg(err);
  }

}


void
Node::create_chains( const AlterTopo & topo,
                     ControlMessage & response,
                     const std::vector<operator_id_t>& toStart,
                     vector< OperatorChain* >& newChains,
                     map<operator_id_t, boost::shared_ptr<OperatorChain> >& opToChain) throw(operator_err_t) {
  std::map<operator_id_t, const Edge *> op_to_outedge;
  std::map<operator_id_t, operator_id_t> op_to_pred;
  for (int i=0; i < topo.edges_size(); ++i) {
    const Edge &edge = topo.edges(i);
    
    if (edge.has_src()) {
      operator_id_t src (edge.computation(), edge.src());
      shared_ptr<COperator> srcOperator = get_operator(src);
      
      if (!srcOperator) {
        LOG(WARNING) << "No operator " << src.to_string() << ", oplist is:\n" << make_op_list();
        throw operator_err_t("unknown source operator " + src.to_string() + " for edge.");
      }

      op_to_outedge[src] = &edge;
      
      if (edge.has_dest()  && !edge.has_dest_addr()) {
        operator_id_t dest (edge.computation(), edge.dest());
        shared_ptr<COperator> destOperator = get_operator(dest);
        if (!destOperator) {
          throw operator_err_t( "Edge from " + src.to_string() + " to unknown dest "+ dest.to_string());
        }
        op_to_pred[dest] = src;
      }
    } else {
      if(!edge.has_src_cube() && ~edge.has_dest())
        throw operator_err_t("edges without src op must connect cubes to operators. Instead, got "  +
             edge.Utf8DebugString());
      
      shared_ptr<DataCube> c = cubeMgr.get_cube(edge.src_cube());
      operator_id_t dest (edge.computation(), edge.dest());

      shared_ptr<cube::Subscriber> destOperator =
         boost::dynamic_pointer_cast<cube::Subscriber>(get_operator(dest));

      ostringstream err_msg;
      
      if (!c) {
        err_msg<< "Can't add edge from " << edge.src_cube() << " to "  << edge.dest() << ": no such cube";
      } else if (!destOperator) {
        err_msg << "Can't add edge from " << edge.src_cube() << " to " 
            << edge.dest() << ": no such destination (or dest not a subscriber)";      
      }
      
      string err_msg_str = err_msg.str();
      if (err_msg_str.length() > 0) {
        throw operator_err_t(err_msg_str);
      }
      c->add_subscriber(destOperator);
    }
  }
  
  int chain_count = 0;
  LOG(INFO) << "building chains";
  for (unsigned i = 0; i < toStart.size(); ++i) {
    shared_ptr<COperator> chainOp = get_operator(toStart[i]);
    bool is_startable = chainOp->is_source();

    if ( op_to_pred.count(toStart[i]) == 0 || is_startable) {
      chain_count += 1;
      boost::shared_ptr<OperatorChain> chain(new OperatorChain);
      LOG_IF(WARNING, !chainOp) << "operator " << toStart[i] << " not in table";
      operator_id_t op_id = toStart[i];
      
      shared_ptr<ChainMember> nextMember = chainOp;

      bool hit_end = false;
      LOG(INFO) << "Found a chain starting from " << op_id;
      do {
        chain->add_member(nextMember);
        nextMember->add_chain(chain);
        
        const Edge* next_e = op_to_outedge[op_id];
        opToChain[op_id] = chain;
        LOG(INFO) << "adding " << nextMember->id_as_str();
        if (!next_e || !dynamic_cast<COperator*>(nextMember.get()) || hit_end) //hit a non-operator
          break;
        
        if (next_e->has_dest_addr()) {
          shared_ptr<RemoteDestAdaptor> xceiver(
            new RemoteDestAdaptor(dataConnMgr, *connMgr, *iosrv, *next_e, config.data_conn_wait) );
          dataConnMgr.register_new_adaptor(xceiver);
          nextMember = xceiver;
        } else if (next_e->has_dest()) {
          op_id = operator_id_t(next_e->computation(), next_e->dest());
          shared_ptr<COperator> nextOp = get_operator(op_id);
          nextMember = nextOp;
          hit_end = nextOp->is_chain_end();
        } else if (next_e->has_dest_cube()) {
          nextMember = get_cube(next_e->dest_cube());
        } else {
          LOG(FATAL) << "no idea what dest is; " << next_e->Utf8DebugString();
        }
      } while ( true );

      if (is_startable) {
        chainSources[toStart[i] ] = chain;
        newChains.push_back(chain.get());
      } else {
        LOG(INFO) << "operator " << toStart[i] << " is start of pending chain";
        sourcelessChain[ toStart[i] ] = chain;
      }
      dataConnMgr.created_chain(chain);
    }
  }
  LOG(INFO) << chain_count << " chains built";
}



void
Node::establish_congest_policies( const AlterTopo & topo,
                                  ControlMessage & resp,
                                  const vector<operator_id_t>& toStart,
                                  map<operator_id_t, boost::shared_ptr<OperatorChain> >& opToChain)
                                    throw(operator_err_t){
  
  map<operator_id_t, bool> operators_with_policies;
  for (int i =0; i < topo.congest_policies_size(); ++ i) {
//    establish_policies(topo.congest_policies(i));
    const CongestPolicySpec& p_spec = topo.congest_policies(i);
    boost::shared_ptr<CongestionPolicy> policy(new CongestionPolicy);
    boost::shared_ptr<COperator> op;
    ostringstream op_list;

    shared_ptr<OperatorChain> chain;
    for (int t = 0;  t < p_spec.op_size(); ++t) {
      operator_id_t id = unparse_id(p_spec.op(t));
      op = get_operator(id);
      if (op) {
        policy->add_operator(id);
        chain = opToChain[id];
        if (!chain)
          throw operator_err_t("Operator " + id.to_string() + " not created in this Alter");
        op->set_congestion_policy(policy);
        operators_with_policies[id] = true;
        op_list << " " << op->id_as_str();
      } else {
        LOG(ERROR) << "can't set policy for nonexistent operator " << id;
        continue;
      }
    }
    policy->set_chain(chain);
    string mon_name = chain->congestion_monitor() ? chain->congestion_monitor()->name()
        : "no-monitor";
    LOG(INFO) << "Policy " << i << ":" << op_list.str() << " " << mon_name;
  }
  
  for (unsigned int i = 0; i < toStart.size(); ++i) {
    if (operators_with_policies.find(toStart[i]) == operators_with_policies.end()) {
      boost::shared_ptr<CongestionPolicy> policy(new CongestionPolicy);
      boost::shared_ptr<OperatorChain> chain = opToChain[toStart[i]];

      if (!chain) {
        throw operator_err_t("Can't add policy to " + toStart[i].to_string() + "; no chain with operator.");
      }
      policy->add_operator(toStart[i]);
      shared_ptr<CongestionMonitor> mon = chain->congestion_monitor();
      policy->set_congest_monitor(mon);
      policy->set_chain(chain);
      
      boost::shared_ptr<COperator> op = get_operator(toStart[i]);
      if (!op) {
        LOG(ERROR)  << "can't set policy for nonexistent operator " << toStart[i];
        continue;
      }
      op->set_congestion_policy(policy);
      string monitor_name = "undefined";
      if(mon) {
        monitor_name = "defined";
      }
      LOG(INFO) << "added default congestion policy for " << toStart[i]<<
        ". Monitor is " << monitor_name;
    }

  }
}



void
Node::make_stop_comput_response(ControlMessage& response, std::vector<int32_t> stopped_operators, int32_t compID) {
  response.set_type(ControlMessage::ALTER_RESPONSE);
  AlterTopo *respTopo = response.add_alter();
  respTopo->set_computationid(compID);
  for (unsigned int i = 0; i < stopped_operators.size(); ++i) {
    TaskID * tID = respTopo->add_tasktostop();
    tID->set_computationid(compID);
    tID->set_task(stopped_operators[i]);
  }
}

void purgeChains(int compID, map<operator_id_t,
                 shared_ptr<OperatorChain> >& chainMap,
                 vector< shared_ptr<OperatorChain> >& chains_to_stop,
                 vector<int32_t>& stopped_ops) {
    std::map<operator_id_t, shared_ptr<OperatorChain> >::iterator iter;
    for ( iter = chainMap.begin(); iter != chainMap.end(); iter++) {
      operator_id_t op_id = iter->first;
      boost::shared_ptr<OperatorChain>  op = iter->second;    
      if (op_id.computation_id == compID) {
        op->stop();
        stopped_ops.push_back(op_id.task_id);
        LOG(INFO) << "erasing " << op_id;
      }
    }

}

vector<int32_t>
Node::stop_computation(int32_t compID) {
  LOG(INFO) << "stopping computation " << compID;
  vector<int32_t> stopped_ops;
  vector< shared_ptr<OperatorChain> > chains_to_stop;
  
  {
    shared_lock<boost::shared_mutex> lock(operatorTableLock);
    purgeChains(compID, sourcelessChain, chains_to_stop, stopped_ops);
    chains_to_stop.clear(); //ignore non-started chains.
    purgeChains(compID, chainSources, chains_to_stop, stopped_ops);
  }
  
  for(unsigned i = 0; i < chains_to_stop.size(); ++i)
    chains_to_stop[i]->stop();
  
  {
    unique_lock<boost::shared_mutex> lock(operatorTableLock);
    std::map<operator_id_t, weak_ptr<COperator> >::iterator iter;
    for ( iter = operators.begin(); iter != operators.end(); ) {
      operator_id_t oid = iter->first;
      bool should_del = iter->second.expired() || oid.computation_id == compID;
      iter ++;
      if (should_del) {
        operators.erase(oid);
        sourcelessChain.erase(oid);
        chainSources.erase(oid);
      }
    }
  }
  LOG(INFO) << "stopped " << stopped_ops.size() << " operators for computation " <<
    compID << " , leaving " << operators.size()<<":\n" << make_op_list();
  return stopped_ops ;
}

boost::shared_ptr<COperator> 
Node::get_operator (operator_id_t name) {
  shared_lock<boost::shared_mutex> lock(operatorTableLock);
  
//  std::map<operator_id_t, weak_ptr<COperator> >::iterator iter;
  weak_ptr<COperator> p = operators[name];
  return p.lock();
}

/**
 Invoked in the strand from the control connection, so needs not be thread-safe
*/
shared_ptr<COperator>
Node::create_operator (string op_typename, operator_id_t name, map<string,string> cfg)
throw(operator_err_t)
{
  
  if (operators.count(name) > 0) {
    throw operator_err_t("operator "  + name.to_string() + "  already exists");
  }

  shared_ptr<COperator> d (operator_loader.newOp(op_typename));
  if (d == NULL) {
    throw operator_err_t("Loader failed to create operator of type " + op_typename);
  }
  d->set_id(name);
  d->set_node(this);
  VLOG(1) << "configuring " << name << " of type " << op_typename;
  operator_err_t err;
  try {
    err = d->configure(cfg);
  } catch(std::exception& e) {
    throw operator_err_t(e.what());
  }
  
  if (err != NO_ERR) {
    LOG(WARNING) << "Failing on exception " << err;
    throw(err);
  }
  return d;
}

bool 
Node::unregister_chain(shared_ptr<OperatorChain> c) {
  unique_lock<boost::shared_mutex> lock(operatorTableLock);
  LOG(INFO) << "node unregistering chain";

  shared_ptr<ChainMember> m = c->member(0);
  shared_ptr<COperator> op = dynamic_pointer_cast<COperator>(m);
  if (op) {
    operator_id_t id = op->id();
    operators.erase(id);
    chainSources.erase(id);
  }
    
  return false; 
//  return delCount > 0;
}



std::string
Node::make_op_list() {
  shared_lock<boost::shared_mutex> lock(operatorTableLock);

  ostringstream s;
  std::map<operator_id_t, weak_ptr<COperator> >::iterator iter;
  for ( iter = operators.begin(); iter != operators.end(); ++iter) {
    operator_id_t op_id = iter->first;
    boost::shared_ptr<COperator>  op = iter->second.lock();
    if (op)
      s << "\t" << op_id << " " << op->typename_as_str() << endl;
    else
      s << "\t" << op_id << " NULL" << endl;

  }
  return s.str();
}



NodeConfig::NodeConfig ()
    : dataplane_ep("0.0.0.0", 0), webinterface_port (0),
    heartbeat_time (0), thread_pool_size (1), data_conn_wait(5000),send_queue_size(1E6),
    cube_db_host("localhost"), cube_db_user("root"), cube_db_pass(""), cube_db_name("test_cube"),
    cube_processor_threads(1), cube_congestion_process_limit(10000), cube_congestion_flush_limit(1000000),
    cube_mysql_innodb(true), cube_mysql_engine_memory(false),cube_mysql_transactions(false), cube_mysql_insert_batch_pw2(8),
    cube_mysql_query_batch_pw2(5), cube_max_stage(10), connection_buffer_size(512 * 1024)
    {}


