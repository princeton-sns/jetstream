/***
* Header for the cube manager component. This subsystem of the worker is responsible
* for allocating, storing, etc the data cubes.
*/

#ifndef JetStream_cube_manager_h
#define JetStream_cube_manager_h

#include "cube.h"
#include <boost/shared_ptr.hpp>
#include <map>

namespace jetstream {

/**
* Responsible for local allocation and management of data cubes. Cubes are stored
* in a table, listed by name.
*/
class CubeManager {
 private:
  std::map<string, shared_ptr<DataCube> > cubeDict;

 public:
  CubeManager ();

  shared_ptr<DataCube> get_cube (std::string s) { return cubeDict[s]; }
  
  void put_cube (std::string s, boost::shared_ptr<DataCube> c) {
    cubeDict.insert( std::pair<string, boost::shared_ptr<DataCube> >(s, c) ); 
  }

  boost::shared_ptr<DataCube> create_cube(std::string name, std::string schema) {
    boost::shared_ptr<DataCube> c(new DataCube(schema));
    cubeDict.insert(name, c);
    return c;
  }
  
};

}

#endif
