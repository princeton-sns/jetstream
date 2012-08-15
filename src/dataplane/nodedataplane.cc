#include <boost/thread/thread.hpp>
#include <boost/date_time.hpp>

#include "nodedataplane.h"

#include "jetstream_controlplane.pb.h"

using namespace jetstream;


// Global configuration variables

port_t jetstream::dataplane_port;   // Host byte order
std::string jetstream::dataplane_config_file;


NodeDataPlane::NodeDataPlane ()
  : alive (false),
    iosrv (new boost::asio::io_service()),
    uplink (new ConnectionToController(*iosrv, tcp::resolver::iterator())) 
{
}


NodeDataPlane::~NodeDataPlane () 
{
}

void
NodeDataPlane::start_heartbeat_thread ()
{
  hb_loop x = hb_loop(uplink);
  boost::thread hb_thread = boost::thread(x);
}


void
NodeDataPlane::connect_to_master ()
{
  
  std::string domain = "localhost";
  // int portno = 3456;

  boost::asio::io_service io_service;
  //should do select loop up here, and also create an acceptor...

  //find the controller
  
  tcp::resolver resolver(io_service);
  tcp::resolver::query query(domain, boost::lexical_cast<std::string> (dataplane_port));
  tcp::resolver::iterator server_side = resolver.resolve(query);
  
  boost::shared_ptr<ConnectionToController> tmp (new ConnectionToController(io_service, server_side));
  uplink = tmp;
  
  boost::thread select_loop(boost::bind(&boost::asio::io_service::run, &io_service));
}



void
hb_loop::operator () ()
{
  
  std::cout << "HB thread started" << std::endl;
  // Connect to server
  while (true) {
    ServerRequest r;
    Heartbeat * h = r.mutable_heartbeat();
    h->set_cpuload_pct(0);
    h->set_freemem_mb(1000);
    uplink->write(&r);
    std::cout << "HB looping" << std::endl;
    boost::this_thread::sleep(boost::posix_time::seconds(HB_INTERVAL));
  }

}


void
ConnectionToController::process_message (char * buf, size_t sz)
{
  std::cout << "got message from master" << std::endl;  
}
