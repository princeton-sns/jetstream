#include <stdlib.h>
#include <iostream>
#include <sstream>
#include <stdexcept>
#include <vector>
#include <fstream>
#include <boost/tokenizer.hpp>
#include <boost/make_shared.hpp>
#include "jetstream_types.pb.h"
	

#include "mysql/mysql_cube.h"

using namespace std;
using namespace jetstream;


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

boost::shared_ptr<jetstream::CubeSchema> get_schema()
{

      boost::shared_ptr<jetstream::CubeSchema> sc = boost::make_shared<jetstream::CubeSchema>();

      jetstream::CubeSchema_Dimension * dim = sc->add_dimensions();
      dim->set_name("time");
      dim->set_type(Element_ElementType_TIME);

      dim = sc->add_dimensions();
      dim->set_name("url");
      dim->set_type(Element_ElementType_STRING);

      dim = sc->add_dimensions();
      dim->set_name("response_code");
      dim->set_type(Element_ElementType_INT32);

      jetstream::CubeSchema_Aggregate * agg = sc->add_aggregates();
      agg->set_name("count");
      agg->set_type("count");

      agg = sc->add_aggregates();
      agg->set_name("avg_size");
      agg->set_type("avg");
      return sc;
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
  std::ifstream myfile (argv[1]);


  cout << "Starting performance test..." << endl;
  cout << endl;
  
  boost::shared_ptr<jetstream::CubeSchema> sc = get_schema();
  boost::shared_ptr<jetstream::cube::MysqlCube> cube =   boost::make_shared<jetstream::cube::MysqlCube>(*sc, "web_requests", true);
  cube->set_batch(batch);

  cube->destroy();
  cube->create();

  std::string line;
  std::string time;
  std::string url;
  int rc;
  int size;
  if (myfile.is_open())
  {
    jetstream::Tuple t;
    t.add_e(); //time
    t.add_e(); //url
    t.add_e(); //response code
    t.add_e(); //size
    
    jetstream::Element *e;
    struct tm temptm;

    while ( myfile.good())
    {
      getline (myfile,line);
      if (line.size()<10)
        continue;
      parse (line, time, url, rc, size); 
      e=t.mutable_e(0);
      if(strptime(time.c_str(),"%d/%b/%Y:%H:%M:%S", &temptm)!= NULL)
      {
        e->set_t_val(mktime(&temptm));
      }
      else
      {
        LOG(ERROR)<<"Error in time conversion: " << time;
      }

      e=t.mutable_e(1);
      e->set_s_val(url);
      e=t.mutable_e(2);
      e->set_i_val(rc);
      e=t.mutable_e(3);
      e->set_i_val(size);

      cube->insert_entry(t);
    }
    myfile.close();
  }
  cout << "Done." << endl;
  return EXIT_SUCCESS;
}

