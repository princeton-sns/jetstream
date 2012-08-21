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
wfile.open("/tmp/access_log_write_wfsync");
if (myfile.is_open())
{
  while ( myfile.good())
  {
    getline (myfile,line);
    
    typedef boost::tokenizer<boost::char_separator<char> >  tokenizer;
    boost::char_separator<char> sep(" ");
   tokenizer tokens(line, sep);
    
   // the following is slower;
    //typedef boost::tokenizer<> tokenizer;
    //tokenizer tokens(line);

    int i = 0;
    for (tokenizer::iterator tok_iter = tokens.begin();
        tok_iter != tokens.end(); ++tok_iter)
    {
      i += 1;
      if(i==4)
      {
        time = *tok_iter;
        time.erase(0,1);
      }
      if (i==7)
      {
        url = *tok_iter;
      }
      if(i==9)
      {
        rc = atoi((*tok_iter).c_str());
      }
      if (i==10)
      {
        if((*tok_iter).compare("-") ==0)
        {
          size = 0;
        }
        else
        {
          size = atoi((*tok_iter).c_str());
        }
      }
      //if(i==4 || i==7 || i==9 || i==10)
      //std::cout << i << "<" << *tok_iter << "> " << endl;
    }
    //cout<< url << " " << time << " " << rc << " "<< size<<endl; 
    wfile << url << " " << time << " " << rc << " "<< size<<endl;
    wfile.flush();
    fsync(wfile.rdbuf()->fd());
  }
  wfile.close();
  myfile.close();
}
    cout << "Done." << endl;
    return EXIT_SUCCESS;
}

