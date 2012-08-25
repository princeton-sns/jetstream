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

void parse_old (const std::string &line, std::string & time, std::string & url, int & rc, int & size)
{
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

}

void parse (const std::string &line, std::string & time, std::string & url, int & rc, int & size)
{
    size_t found = 0;
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
    }

}


	
int main(int argc, const char **argv)
{	

if(argc < 3)
{
  cout<< "need 2 arguments"<< endl;
  exit(1);
}

bool parse_new = true;
std::string line;
std::string time;
std::string url;
int rc;
int size;
std::ifstream myfile (argv[1]);
std::ofstream wfile;
wfile.open(argv[2]);
if (myfile.is_open())
{
  while ( myfile.good())
  {
    getline (myfile,line);
    if (line.size()<10)
	    continue;
    if(parse_new)
      parse(line, time, url, rc, size);
    else
      parse_old(line, time, url, rc, size);
    //cout<< url << " " << time << " " << rc << " "<< size<<endl; 
    wfile << url << " " << time << " " << rc << " "<< size<<endl;
    //wfile.flush();
  }
  wfile.close();
  myfile.close();
}
    cout << "Done." << endl;
    return EXIT_SUCCESS;
}

