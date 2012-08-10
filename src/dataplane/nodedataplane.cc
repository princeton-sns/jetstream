#include <boost/thread/thread.hpp>
#include <boost/date_time.hpp>

#include "nodedataplane.h"

#include "jetstream_controlplane.pb.h"


using namespace std;
using namespace edu::princeton::jetstream;

jetstream::NodeDataPlane::~NodeDataPlane() 
{

}

void
jetstream::NodeDataPlane::start_heartbeat_thread()
{
  hb_loop x= hb_loop(uplink);
  boost::thread hb_thread = boost::thread(x);
  
  
}


void
jetstream::NodeDataPlane::connect_to_master()
{
  std::string domain = "localhost";
  int portno = 3456;

  boost::asio::io_service io_service;
//should do select loop up here, and also create an acceptor...

  //find the controller
  
  tcp::resolver resolver(io_service);
  tcp::resolver::query query(domain, boost::lexical_cast<string>(portno));//no flags
  tcp::resolver::iterator server_side = resolver.resolve(query);

  
  this->uplink = new ConnectionToController(io_service, server_side);
  
  boost::thread select_loop(boost::bind(&boost::asio::io_service::run, &io_service));
}

void
jetstream::hb_loop::operator()()
{
  
  cout << "HB thread started" <<endl;
  //connect to server
  while( true ) {
    ServerRequest r;
    Heartbeat * h = r.mutable_heartbeat();
    h->set_cpuload_pct(0);
    h->set_freemem_mb(1000);
    uplink -> write(r);
    cout << "HB looping" << endl;
    boost::this_thread::sleep(boost::posix_time::seconds(HB_INTERVAL));
  }

}


void
jetstream::ConnectionToController::processMessage(protobuf::Message &msg)
{
  cout << "got message from master" <<endl;  
}