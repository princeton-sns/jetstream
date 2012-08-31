/***
* Header for the cube manager component. This subsystem of the worker is responsible
* for allocating, storing, etc the data cubes.
*/

#ifndef JetStream_cube_manager_h
#define JetStream_cube_manager_h

#include "cube.h"
#include "mysql/cube.h"
#include <boost/shared_ptr.hpp>
#include <map>

namespace jetstream {

/**
* Responsible for local allocation and management of data cubes. Cubes are stored
* in a table, listed by name.
*/
class CubeManager {
 private:
  std::map<std::string, boost::shared_ptr<DataCube> > cubeDict;

 public:
  CubeManager ();

  boost::shared_ptr<DataCube> get_cube (std::string s) { return cubeDict[s]; }
  
  void put_cube (std::string s, boost::shared_ptr<DataCube> c) {
  //TODO need locking here.
    cubeDict.insert( std::pair<std::string, boost::shared_ptr<DataCube> >(s, c) ); 
  }

  boost::shared_ptr<DataCube> create_cube(std::string name, jetstream::CubeSchema schema) {
    boost::shared_ptr<DataCube> c(new cube::MysqlCube(schema));
    put_cube(name, c);
    return c;
  }
  
  void destroy_cube(std::string s) {
      cubeDict[s]->mark_as_deleted();
      cubeDict.erase(s);
    }
  
};

}

#endif
