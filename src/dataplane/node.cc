#include <boost/thread/thread.hpp>
#include <boost/thread/mutex.hpp>
#include <boost/date_time.hpp>

#include "node.h"

#include "jetstream_controlplane.pb.h"

using namespace jetstream;
using namespace std;
using namespace boost;

Node::Node (const NodeConfig &conf)
  : config (conf),
    iosrv (new asio::io_service()),
    conn_mgr (new ClientConnectionManager(iosrv)),
    liveness_mgr (new LivenessManager (iosrv, conf.heartbeat_time)),
    // uplink (new ConnectionToController(*iosrv, tcp::resolver::iterator()))
    // XXX This should get set through config files
    operator_loader ("src/dataplane/") //NOTE: path must end in a slash
{
//Create logger first thing


//Set up the network connection
  asio::io_service::work work(*iosrv);

  if (conf.heartbeat_time > 0) {
    // XXX Can't dynamically modify heartbeat_time. Pass pointer to config?
    for (u_int i=0; i < config.controllers.size(); i++) {
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

  cout << "Finished node::run" << endl;
}


void
Node::stop ()
{
  liveness_mgr->stop_all_notifications();
  iosrv->stop();
}


void
Node::controller_connected (shared_ptr<ClientConnection> dest, 
			    system::error_code error)
{
  if (error)
    cerr << "Node: Monitoring connection failed: " << error.message() << endl;
  else if (!liveness_mgr)
    cerr << "Node: Liveness manager NULL" << endl;
  else {
    {
      mutex::scoped_lock sl;
      cout << "Node: Connected to controller: " << dest->get_endpoint() << endl;
    }
    liveness_mgr->start_notifications(dest);
  }
}


#if 0
void
Node::start_heartbeat_thread ()
{
  hb_loop x = hb_loop(uplink);
  thread hb_thread = thread(x);
}


void
Node::connect_to_master ()
{
  asio::io_service io_service;
  //should do select loop up here, and also create an acceptor...

  if (!config.controllers.size()) {
    cerr << "No controllers known." << endl;
    return;
  }
  pair<string, port_t> address = config.controllers[0];

  tcp::resolver resolver(io_service);
  tcp::resolver::query query(address.first, lexical_cast<string> (address.second));
  tcp::resolver::iterator server_side = resolver.resolve(query);
  
  shared_ptr<ConnectionToController> tmp (new ConnectionToController(io_service, server_side));
  uplink = tmp;
  
  thread select_loop(bind(&asio::io_service::run, &io_service));
}



void
hb_loop::operator () ()
{
  
  cout << "HB thread started" << endl;
  // Connect to server
  while (true) {
    ServerRequest r;
    Heartbeat * h = r.mutable_heartbeat();
    h->set_cpuload_pct(0);
    h->set_freemem_mb(1000);
    uplink->write(&r);
    cout << "HB looping" << endl;
    this_thread::sleep(posix_time::seconds(HB_INTERVAL));
  }

}
#endif

void
ConnectionToController::process_message (char * buf, size_t sz)
{
  cout << "got message from master" << endl;  
}

operator_id_t unparse_id(TaskID id) {
  operator_id_t parsed;
  parsed.computation_id = id.computationid();
  parsed.task_id = id.task();
  return parsed;
}

void
Node::handle_alter(AlterTopo topo)
{
  map<operator_id_t, map<string,string> > operator_configs;
  for (int i=0; i < topo.tostart_size(); ++i) {
    TaskMeta task = topo.tostart(i);
    operator_id_t id = unparse_id(task.id());
    string cmd = task.op_typename();
    map<string,string> config;
    for (int j=0; j < task.config_size(); ++j) {
      TaskMeta_DictEntry cfg_param = task.config(j);
      config[cfg_param.opt_name()] = cfg_param.val();
    }
    operator_configs[id] = config;
    create_operator(cmd, id);
     //TODO: what if this returns a null pointer, indicating create failed?
  }
  
  //make cubes here
  for (int i=0; i < topo.tocreate_size(); ++i) {
    CubeMeta task = topo.tocreate(i);
    cube_mgr.create_cube(task.name(), task.schema());
  }
  
  //TODO remove cubes and operators if specified.
  
  
  
  
    //add edges
  for (int i=0; i < topo.edges_size(); ++i) {
    const Edge& e = topo.edges(i);
    operator_id_t src( e.computation(), e.src());
    shared_ptr<DataPlaneOperator> src_op = get_operator(src);
    
    if (e.has_cube_name()) {     //connect to local table
      shared_ptr<DataCube> d = cube_mgr.get_cube(e.cube_name());
      src_op->set_dest(d);
    } else if (e.has_dest_addr()) {   //remote network operator
      //TODO handle network
    } else {
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
}


shared_ptr<DataPlaneOperator>
Node::create_operator(string op_typename, operator_id_t name) {
  shared_ptr<DataPlaneOperator> d( operator_loader.newOp(op_typename));

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

