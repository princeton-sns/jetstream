#include <boost/thread/thread.hpp>
#include <boost/date_time.hpp>

#include "node.h"

#include "jetstream_controlplane.pb.h"

using namespace jetstream;
using namespace std;
using namespace boost;

Node::Node (const NodeConfig &conf)
  : config (conf),
    alive (false),
    iosrv (new asio::io_service()),
    uplink (new ConnectionToController(*iosrv, tcp::resolver::iterator())) ,
    operator_loader("src/dataplane/") //NOTE: path must end in a slash
{
}


Node::~Node () 
{
}

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
  pair<string, string> address = config.controllers[0];

  tcp::resolver resolver(io_service);
  tcp::resolver::query query(address.first, address.second);
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
  
    //add edges
  for (int i=0; i < topo.edges_size(); ++i) {
    Edge e = topo.edges(i);
    operator_id_t src( e.computation(), e.src());
    operator_id_t dest( e.computation(), e.dest());
    shared_ptr<DataPlaneOperator> src_op = get_operator(src);
    shared_ptr<DataPlaneOperator> dest_op = get_operator(dest);
    src_op->set_dest(dest_op.get());
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

