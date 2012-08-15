//#include <boost/format.hpp>
//#include <boost/algorithm/string.hpp>
#include <boost/program_options.hpp>
#include "js_defs.h"
#include "js_version.h"
#include "nodedataplane.h"

using namespace jetstream;

// Return 0 on success, -1 on failure
static int
parse_config (boost::program_options::variables_map *inputopts,
	      int argc, char **argv)
{
  namespace popts = boost::program_options;

  // Options input from command line
  popts::options_description cmd_opts("Command line options");
  cmd_opts.add_options()
    ("version,V", "print version string")
    ("help,h", "produce help message")
    ("config,C", popts::value<std::string>(), "supply config file location")
    ("port,p", popts::value<u_int>(), "set dataplane port number")
    ("start", "start program")
    ("restart", "restart program")
    ("stop", "stop program")
    ;
  
  // Options from both cmd line and config file
  popts::options_description conf_opts("Configuration options");
  conf_opts.add_options()
    ("help,h", "produce help message")
    ("hbtimer", popts::value<long>(), "set heartbeat timer")
    ("port", popts::value<port_t>(), "set dataplane port number")
    ;
  
  // Build set of all allowable options
  popts::options_description opts("Allowed options");
  opts.add(cmd_opts).add(conf_opts);
  
  popts::variables_map &input_opts = *inputopts;

  try {
    popts::store(popts::parse_command_line(argc, argv, cmd_opts), input_opts);
  }
  catch (const std::exception& e) {
    std::cerr << e.what() << std::endl;
    std::cout << opts << std::endl;
    return 1;
  };
  
  if (input_opts.count("help")) {
    std::cout << opts << std::endl;
    return 1;
  }
  
  if (input_opts.count("version")) {
    std::cout << getprogname() << ": vers " << JETSTREAM_VERSION << std::endl;
    return 1;
  }
  
  // Must have at least one command
  if (!input_opts.count("restart") 
      && !input_opts.count("start")
      && !input_opts.count("stop")) {
    std::cout << getprogname() << ": option (start|stop|restart) missing" 
	      << std::endl;
    std::cout << opts << std::endl;
    return 1;
  }
  

  if (input_opts.count ("config"))
    dataplane_config_file = input_opts["config"].as<std::string>();
  else {
    std::cout << getprogname() << ": configuration file missing"
	      << std::endl;
    return 1;
  }

  try {
    //    popts::store(popts::parse_config_file(dataplane_config_file, opts),
    popts::store(popts::parse_config_file<char>(dataplane_config_file.c_str(), opts),
		 input_opts);
    popts::notify(input_opts);
  }
  catch (const std::exception& e) {
    std::cerr << e.what() << std::endl;
    std::cout << opts << std::endl;
    return 1;
  };

  // Configuration variables
  if (input_opts.count("port"))
    dataplane_port = input_opts["port"].as<port_t>();

  return 0;
}


static void
jsnode_start ()
{
  NodeDataPlane t;
  t.connect_to_master();
  t.start_heartbeat_thread();

  //create network interface here?
  // t.start_heartbeat_thread(iface);
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

  boost::program_options::variables_map input_opts;

  int rc = parse_config (&input_opts, argc, argv);
  if (rc)
    exit(1);

  if (input_opts.count("restart")) {
    jsnode_stop();
    jsnode_start();
  }
  else if (input_opts.count("stop"))
    jsnode_stop();
  else if (input_opts.count("start"))
    jsnode_start();
  else {
    std::cout << getprogname() 
	      << "Missing appropriate start command" << std::endl;
    exit(1);
  }
    
  exit(0);
}
