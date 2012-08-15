//#include <boost/format.hpp>
//#include <boost/algorithm/string.hpp>
#include <boost/program_options.hpp>
#include <boost/algorithm/string.hpp>

#include "js_defs.h"
#include "js_version.h"
#include "nodedataplane.h"

using namespace jetstream;
using namespace ::std;
using namespace boost;
using namespace program_options;

// Return 0 on success, -1 on failure
static int
parse_config (program_options::variables_map *inputopts,
	      int argc, char **argv,
	      NodeDataPlaneConfig &config)
{
  // Options input from command line
  options_description cmd_opts("Command line options");
  cmd_opts.add_options()
    ("version,V", "print version string")
    ("help,h", "produce help message")
    ("config,C", value<string>(), "supply config file location")
    ("start", "start program")
    ("restart", "restart program")
    ("stop", "stop program")
    ;
  
  // Options from both cmd line and config file
  options_description conf_opts("Configuration file and command line options");
  conf_opts.add_options()
    ("hbtimer,t", value<long>(), 
     "liveness monitoring timer (in milliseconds)")
    ("controller_addr,a", value<vector<string> >()->composing(),
     "hostname:port of controller (can supply multiple entries)")
    ("dataplane_port,d", value<u_int16_t>(), 
     "my dataplane port number")
    ("controlplane_port,c", value<u_int16_t>(), 
     "my controlplane port number")
    ;
  
  // Build set of all allowable options
  options_description opts("Allowed options");
  opts.add(cmd_opts).add(conf_opts);
  
  variables_map &input_opts = *inputopts;

  try {
    store(parse_command_line(argc, argv, cmd_opts), input_opts);
  }
  catch (const std::exception &e) {
    cerr << e.what() << endl;
    cout << opts << endl;
    return 1;
  };
  
  if (input_opts.count("help")) {
    cout << opts << endl;
    return 1;
  }
  
  if (input_opts.count("version")) {
    cout << getprogname() << ": vers " << JETSTREAM_VERSION << endl;
    return 1;
  }
  
  // Must have at least one command
  if (!input_opts.count("restart") 
      && !input_opts.count("start")
      && !input_opts.count("stop")) {
    cout << getprogname() << ": option (start|stop|restart) missing" 
	 << endl;
    cout << opts << endl;
    return 1;
  }
  

  if (input_opts.count ("config"))
    config.config_file = input_opts["config"].as<string>();
  else {
    cout << getprogname() << ": configuration file missing"
	 << endl;
    return 1;
  }

  try {
    store(parse_config_file<char> (config.config_file.c_str(), opts),
	  input_opts);
    notify(input_opts);
  }
  catch (const std::exception &e) {
    cerr << e.what() << endl;
    cout << opts << endl;
    return 1;
  };

  // Configuration variables
  if (input_opts.count("controlplane_port"))
    config.controlplane_myport = input_opts["controlplane_port"].as<port_t>();

  if (input_opts.count("dataplane_port"))
    config.dataplane_myport = input_opts["dataplane_port"].as<port_t>();

  // Configuration variables
  if (input_opts.count("controller_addrs")) {
    vector<string> addrs = input_opts["controller_addrs"].as<vector<string> >();
    if (!addrs.size()) {
      cerr << getprogname() << ": no controller addresses given" << endl;
      cout << opts << endl;
      return 1;
    }

    for (u_int i=0; i < addrs.size(); i++) {
      const string &addr = addrs[i];
      vector<string> a;
      split(a, addr, is_any_of(":"));
    
      if (a.size() != 2) {
	cerr << getprogname() << ": incorrect format for controller address:"
	     << addr << endl;
	cout << opts << endl;
	return 1;
      }

      long tmpport = lexical_cast<long> (a[1]);
      if (tmpport > MAX_UINT16) {
	cerr << getprogname() << ": invalid port for controller address"
	     << addr << endl;
	cout << opts << endl;
	return 1;
      }
      
      pair<string, string> p (a[0], a[1]);
      config.controllers.push_back (p);
    }
  }

  return 0;
}


static void
jsnode_start (NodeDataPlaneConfig &config)
{
  NodeDataPlane t (config);
  t.connect_to_master();
  t.start_heartbeat_thread();

  //create network interface here?
  // t.start_heartbeat_thread(iface);
  // hb_loop loop = hb_loop(controller_conn);
  //loop();
  //end of app; fall off and exit
  cout << "exiting cleanly" << endl;
}


static void
jsnode_stop ()
{

}



int
main (int argc, char **argv)
{
  setprogname(argv[0]);

  NodeDataPlaneConfig config;
  variables_map input_opts;

  int rc = parse_config (&input_opts, argc, argv, config);
  if (rc)
    exit(1);

  if (input_opts.count("restart")) {
    jsnode_stop();
    jsnode_start(config);
  }
  else if (input_opts.count("stop"))
    jsnode_stop();
  else if (input_opts.count("start"))
    jsnode_start(config);
  else {
    cout << getprogname() 
	 << "Missing appropriate start command" << endl;
    exit(1);
  }
    
  exit(0);
}
