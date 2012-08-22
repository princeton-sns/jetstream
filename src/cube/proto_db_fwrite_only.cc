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
std::string time;
std::string url;
int rc;
int size;
std::ifstream myfile ("/tmp/access_log");
std::ofstream wfile;
size_t found;
wfile.open("/tmp/access_log_write_wfsync");
if (myfile.is_open())
{
  while ( myfile.good())
  {
    getline (myfile,line);
    if (line.size()<10)
	continue;
    found = 0;
    
    int i = 0;
    while(i<11)
    {
      i += 1;
      found = line.find_first_of(" ", found+1);
      if(i==3)
      {
        time = line.substr(found+2, line.find_first_of(" ", found+1)-(found+2)); 
      }
      if (i==6)
      {
        url = line.substr(found+1, line.find_first_of(" ", found+1)-(found+1)); 
      }
      if(i==8)
      {
        rc = atoi(line.substr(found+1, line.find_first_of(" ", found+1)-(found+1)).c_str());
      }
      if (i==9)
      {
        if(line[found+1] == '-')
        {
          size = 0;
        }
        else
        {
          size = atoi(line.substr(found+1, line.find_first_of(" ", found+1)-(found+1)).c_str());
        }
      }
      //if(i==4 || i==7 || i==9 || i==10)
      //std::cout << i << "<" << *tok_iter << "> " << endl;
    }
    //cout<< url << " " << time << " " << rc << " "<< size<<endl; 
    wfile << url << " " << time << " " << rc << " "<< size<<endl;
    wfile.flush();
    //fsync(wfile.rdbuf()->fd());
  }
  wfile.close();
  myfile.close();
}
    cout << "Done." << endl;
    return EXIT_SUCCESS;
}

