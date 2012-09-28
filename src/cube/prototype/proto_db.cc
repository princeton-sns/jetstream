#include <stdlib.h>
#include <iostream>
#include <sstream>
#include <stdexcept>
#include <vector>
#include <fstream>
#include <boost/tokenizer.hpp>
	
#include "mysql_connection.h"
#include "mysql_driver.h"
	
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
  //keep these options to fastest combo in svn
  bool use_sp = false;
  bool convert_time = false;
  bool parse_new = true;

  string url(argc >= 2 ? argv[1] : EXAMPLE_HOST);
  const string user(argc >= 3 ? argv[2] : EXAMPLE_USER);
  const string pass(argc >= 4 ? argv[3] : EXAMPLE_PASS);
  const string database(argc >= 5 ? argv[4] : EXAMPLE_DB);
  char timestring[30];
  string time_input_sql;

  cout << "Connector/C++ tutorial framework..." << endl;
  cout << endl;


  try {
    sql::Driver * driver = sql::mysql::get_driver_instance();
    sql::Statement *stmt;
    std::auto_ptr<sql::Connection > con(driver->connect(url, user, pass));
    con->setSchema(database);

    stmt = con->createStatement();
    stmt->execute("DELETE FROM logs;");


    std::auto_ptr<sql::PreparedStatement >  pstmt;

    if(convert_time)
      time_input_sql = "?";
    else
      time_input_sql = "STR_TO_DATE(?, '%d/%M/%Y:%H:%i:%s')";

    if(use_sp)
      pstmt.reset(con->prepareStatement("call add_element("+time_input_sql+", ?, ?, ?);"));
    else
      pstmt.reset(con->prepareStatement("INSERT INTO `test_cube`.`logs` \
  (`time`, `time_agg_level`, `url`, `response_code`, `val_count`, `val_sum_avg_rt`, `val_count_avg_rt`) \
  VALUES ("+time_input_sql+", '0', ?, ?, 1, ?, 1) \
ON DUPLICATE KEY UPDATE\
`val_count` = `val_count` + 1,`val_sum_avg_rt` = `val_sum_avg_rt`  + VALUES(`val_sum_avg_rt`),\
`val_count_avg_rt` = `val_count_avg_rt` + 1;"));

    std::string line;
    std::string time;
    std::string url;
    int rc;
    int size;
    std::ifstream myfile ("/tmp/access_log");
    if (myfile.is_open())
    {
      while ( myfile.good())
      {
        getline (myfile,line);
        if (line.size()<10)
          continue;
        if (parse_new)
          parse (line, time, url, rc, size); 
        else
          parse_old (line, time, url, rc, size); 

        //cout<< url << " " << time << " " << rc << " "<< size<<endl; 
        if(convert_time) {
          struct tm temptm;
          strptime(time.c_str(),"%d/%b/%Y:%H:%M:%S", &temptm);
          strftime(timestring, sizeof(timestring)-1, "%Y-%m-%d %H:%M:%S", &temptm);
          pstmt->setString(1, timestring);
        }
        else
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

