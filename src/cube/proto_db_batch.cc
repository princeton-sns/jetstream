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
	
#define HOST "localhost"
#define USER "root"
#define PASS ""
#define DB "test_cube"
	
using namespace std;


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

  string query;
  int batch = atoi(argv[2]);


  cout << "Connector/C++ tutorial framework..." << endl;
  cout << endl;


  try {
    sql::Driver * driver = get_driver_instance();
    std::auto_ptr<sql::Connection > con(driver->connect(HOST, USER, PASS));
    sql::Statement *stmt;
    con->setSchema(DB);

    std::auto_ptr<sql::PreparedStatement >  pstmt;

    stmt = con->createStatement();
    stmt->execute("DELETE FROM logs;");
  
    query = "INSERT INTO `test_cube`.`logs` \
  (`time`, `time_agg_level`, `url`, \
  `response_code`, `val_count`, `val_sum_avg_rt`, \
  `val_count_avg_rt`) VALUES ";
    
    for(int j = 0; j< (batch - 1); ++j)
    {
      query += " (STR_TO_DATE(?, '%d/%M/%Y:%H:%i:%s'), '0', ?, ?, 1, ?, 1), ";
    }
    query += " (STR_TO_DATE(?, '%d/%M/%Y:%H:%i:%s'), '0', ?, ?, 1, ?, 1) \
 ON DUPLICATE KEY UPDATE\
`val_count` = `val_count` + 1,\
`val_sum_avg_rt` = `val_sum_avg_rt`  + VALUES(`val_sum_avg_rt`),\
`val_count_avg_rt` = `val_count_avg_rt` + 1;";


    pstmt.reset(con->prepareStatement(query));

    std::string line;
    std::string time;
    std::string url;
    int rc;
    int size;
    std::ifstream myfile (argv[1]);
    if (myfile.is_open())
    {
      int i = 0;
      while ( myfile.good())
      {
        getline (myfile,line);
        if (line.size()<10)
          continue;
        parse (line, time, url, rc, size); 

        //cout<< url << " " << time << " " << rc << " "<< size<<endl; 
        pstmt->setString((i*4)+1, time);
        pstmt->setString((i*4)+2, url);
        pstmt->setInt((i*4)+3, rc);
        pstmt->setInt((i*4)+4, size);
        ++i;
        if (i >= batch)
        {
          i=0;
          pstmt->execute();            
        }
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

