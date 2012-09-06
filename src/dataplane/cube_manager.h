/***
* Header for the cube manager component. This subsystem of the worker is responsible
* for allocating, storing, etc the data cubes.
*/

#ifndef JetStream_cube_manager_h
#define JetStream_cube_manager_h

#include <boost/shared_ptr.hpp>
#include <boost/thread/mutex.hpp>
#include <map>
#include "mysql_cube.h"
#include "cube.h"

namespace jetstream {

/**
* Responsible for local allocation and management of data cubes. Cubes are stored
* in a table, listed by name.  Access functions (get/put/destory) block on
* lock acquisition for thread safety.
*/
class CubeManager {
 private:
  std::map<std::string, boost::shared_ptr<DataCube> > cubeMap;
  boost::mutex mapMutex;

 public:
  CubeManager () {}

  boost::shared_ptr<DataCube> create_cube (const std::string &name, const CubeSchema &schema);
  boost::shared_ptr<DataCube> get_cube (const std::string &name);
  void put_cube (const std::string &name, boost::shared_ptr<DataCube> c);
  void destroy_cube (const std::string &name);
};

}

#endif
