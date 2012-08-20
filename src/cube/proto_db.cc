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

    string url(argc >= 2 ? argv[1] : EXAMPLE_HOST);
    const string user(argc >= 3 ? argv[2] : EXAMPLE_USER);
    const string pass(argc >= 4 ? argv[3] : EXAMPLE_PASS);
    const string database(argc >= 5 ? argv[4] : EXAMPLE_DB);

    cout << "Connector/C++ tutorial framework..." << endl;
    cout << endl;

	
    try {
	
sql::Driver * driver = get_driver_instance();

std::auto_ptr<sql::Connection > con(driver->connect(url, user, pass));
con->setSchema(database);

std::auto_ptr<sql::PreparedStatement >  pstmt;

//cout << "Executing Query" << endl;
pstmt.reset(con->prepareStatement("CALL add_element(STR_TO_DATE(?, '%d/%M/%Y:%H:%i:%s'),?,?,?);"));
//pstmt->setString(1, "2012-01-01 01:01:01");
//pstmt->setString(2, "http:\\\\www.google.com");
//pstmt->setInt(3, 200);
//pstmt->setInt(4, 200);
//pstmt->execute();            
//cout << "Done with query" << endl;

std::string line;
std::string time;
std::string url;
int rc;
int size;
std::ifstream myfile ("/Users/matveyarye/Downloads/access_log");
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
    pstmt->setString(1, time);
    pstmt->setString(2, url);
    pstmt->setInt(3, rc);
    pstmt->setInt(4, size);
    pstmt->execute();            
  }
  myfile.close();
}


	
    } catch (sql::SQLException &e) {
        /*
          The MySQL Connector/C++ throws three different exceptions:
	
          - sql::MethodNotImplementedException (derived from sql::SQLException)
          - sql::InvalidArgumentException (derived from sql::SQLException)
          - sql::SQLException (derived from std::runtime_error)
        */
        cout << "# ERR: SQLException in " << __FILE__;
        cout << "(" << __FUNCTION__ << ") on line " << __LINE__ << endl;
        /* Use what() (derived from std::runtime_error) to fetch the error message */
        cout << "# ERR: " << e.what();
        cout << " (MySQL error code: " << e.getErrorCode();
        cout << ", SQLState: " << e.getSQLState() << " )" << endl;
	
        return EXIT_FAILURE;
    }
	
    cout << "Done." << endl;
    return EXIT_SUCCESS;
}

