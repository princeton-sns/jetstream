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


void
Node::handle_alter(AlterTopo topo)
{
  map<operator_id_t, map<string,string> > operator_configs;
  for (int i=0; i < topo.tostart_size(); ++i) {
    TaskMeta task = topo.tostart(i);
    TaskID id = task.id();
    string cmd = task.op_typename();
    map<string,string> config;
    for (int j=0; j < task.config_size(); ++j) {
      TaskMeta_DictEntry cfg_param = task.config(j);
      config[cfg_param.opt_name()] = cfg_param.val();
    }
    
    //create operators here
  }
  
  //add edges here
  for (int i=0; i < topo.edges_size(); ++i) {
    
  }  
}


shared_ptr<DataPlaneOperator>
Node::create_operator(string op_typename, operator_id_t name) {
  shared_ptr<DataPlaneOperator> d( operator_loader.newOp(op_typename));
  operators[name] = d;
  return d;
}

