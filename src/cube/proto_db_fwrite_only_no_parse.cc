#include <stdlib.h>
#include <iostream>
#include <sstream>
#include <stdexcept>
#include <vector>
#include <fstream>
#include <boost/tokenizer.hpp>
	
#include "mysql_connection.h"
	
#include <cppconn/driver.h>
#include <cppconn/exception.h>
#include <cppconn/resultset.h>
#include <cppconn/statement.h>
#include <cppconn/prepared_statement.h>
	
#define EXAMPLE_HOST "localhost"
#define EXAMPLE_USER "root"
#define EXAMPLE_PASS ""
#define EXAMPLE_DB "test_cube"
	
using namespace std;
	
int main(int argc, const char **argv)
{	

std::string line;
std::ifstream myfile ("/tmp/access_log");
std::ofstream wfile;
wfile.open("/tmp/access_log_write_wfsync");
if (myfile.is_open())
{
  while ( myfile.good())
  {
    getline (myfile,line);
    wfile << line;
    wfile.flush();
    //fsync(wfile.rdbuf()->fd());
  }
  wfile.close();
  myfile.close();
}
    cout << "Done." << endl;
    return EXIT_SUCCESS;
}

