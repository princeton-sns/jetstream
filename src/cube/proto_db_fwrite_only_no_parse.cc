#include <stdlib.h>
#include <iostream>
#include <sstream>
#include <stdexcept>
#include <vector>
#include <fstream>
#include <fcntl.h>
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
bool use_low_level = false;

if(argc < 3)
{
  cout<< "need 2 arguments"<< endl;
  exit(1);
}

std::string line;
if(use_low_level)
  std::ifstream myfile (argv[1]);
else
{
  std::ofstream wfile;
  wfile.open(argv[2]); 
}

int fdw = open(argv[2], O_WRONLY|O_TRUNC|O_CREAT);
if (myfile.is_open())
{
  while ( myfile.good())
  {
    getline (myfile,line);
    if(use_low_level)
    {
      write(fdw, line.c_str(), line.size()); 
      //write(fdw, "\n", 1); 
      //fsync(fdw);
      //
    }
    else
    {
      wfile << line <<endl;
    }
  }
  if(use_low_level)
    close(fdw);
  else
    wfile.close;
  myfile.close();
}
    cout << "Done." << endl;
    return EXIT_SUCCESS;
}

