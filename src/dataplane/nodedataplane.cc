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
  hb_loop x= hb_loop(iface);
  boost::thread hb_thread = boost::thread(x);
  
  
}


void
jetstream::hb_loop::operator()()
{
  
  cout << "HB thread started" <<endl;
  //connect to server
  while( true ) {
    Heartbeat h;
    h.set_cpuload_pct(0);
    h.set_freemem_mb(1000);
//    iface -> send_pb()
    cout << "HB looping" << endl;
    boost::this_thread::sleep(boost::posix_time::seconds(HB_INTERVAL));
  }

}