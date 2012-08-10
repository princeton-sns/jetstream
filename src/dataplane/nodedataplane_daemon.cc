#include <boost/format.hpp>
#include <boost/algorithm/string.hpp>
#include "nodedataplane.h"
#include <boost/thread/thread.hpp>


using namespace jetstream;

static void
usage ()
{
  std::cout << "Usage: " << getprogname()
	    << " [-C configfile] (start|stop|restart)\n";

  exit(1);
}

static void
jsnode_start (std::string config)
{
  if (!config.size()) {
    //read a default config file here
  }
  NodeDataPlane t;
  t.connect_to_master();
  t.start_heartbeat_thread();

  //create network interface here?
//  t.start_heartbeat_thread(iface);
 // hb_loop loop = hb_loop(controller_conn);
  //loop();
  //end of app; fall off and exit
}


static void
jsnode_stop ()
{

}



int
main (int argc, char **argv)
{
  setprogname(argv[0]);

  std::string config;

  bool start = false;
  bool stop = false;

  for (int i=0; i < argc; i++) {
    if (argv[i] == NULL)
      usage();

    std::string arg = argv[i];

    if (boost::iequals(arg, std::string("start")))
      start = true;
    else if (boost::iequals(arg, std::string("stop")))
      stop = true;
    else if (boost::iequals(arg, std::string("restart")))
      stop = start = true;
    else if (boost::iequals(arg, std::string("-C"))) {
      if ((i+1) >= argc)
        usage();
      
      config = argv[++i];
    }
  }

  if (!stop && !start)
    usage();
  if (stop)
    jsnode_stop();
  if (start)
    jsnode_start(config);
  
  exit(0);
}
