#include "nodedataplane.h"
#include <boost/thread/thread.hpp>
#include <boost/date_time.hpp>


using namespace std;

jetstream::NodeDataPlane::~NodeDataPlane() 
{

}

void
jetstream::NodeDataPlane::start_heartbeat_thread()
{
  hb_loop x;
  boost::thread hb_thread = boost::thread(x);
  
  
}


void
jetstream::hb_loop::operator()()
{
  
  cout << "HB thread started" <<endl;
  boost::this_thread::sleep(boost::posix_time::seconds(5));

}