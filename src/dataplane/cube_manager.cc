#include <boost/thread/locks.hpp>
#include "mysql_cube.h"

#include "cube_manager.h"
#include <boost/regex.hpp>

#include <glog/logging.h>

using namespace boost;
using namespace jetstream;
using namespace ::std;

shared_ptr<DataCube> 
CubeManager::get_cube (const std::string &name) 
{
  lock_guard<boost::mutex> lock (mapMutex);
  return cubeMap[name];
}
  
shared_ptr<DataCube> 
CubeManager::create_cube ( const std::string &name,
                           const CubeSchema &schema,
                           bool overwrite_if_present) {

  static const boost::regex NAME_PAT("[a-zA-Z0-9_]+$");
  lock_guard<boost::mutex> lock (mapMutex);  

  shared_ptr<DataCube> c;
  if (!regex_match(name, NAME_PAT)) {
    LOG(WARNING) << "Invalid cube name caught in dataplane:" <<
          " should be a valid C identifier but got " << name;
    return c;
  }


  std::map<string, shared_ptr<DataCube> >::iterator iter;
  iter = cubeMap.find(name);
  if (iter != cubeMap.end()) { //cube already exists, so return it
    
    return iter->second;
  }


  //TODO: The cube constructor does several things, some of which may fail; we
  //need it to throw an exception in case of failure, which should be caught here
  
  c = shared_ptr<DataCube>(new cube::MysqlCube(schema, name, overwrite_if_present, config.cube_db_host, config.cube_db_user, config.cube_db_pass, config.cube_db_name, config.cube_max_elements_in_batch));
//  set_batch(10)
  c->create();
  if (c != NULL) {
    cubeMap[name] = c;
  }
  return c;
}

void 
CubeManager::destroy_cube (const std::string &name) 
{
  lock_guard<boost::mutex> lock (mapMutex);
  cubeMap[name]->mark_as_deleted();
  cubeMap.erase(name);
}


void
CubeManager::put_cube (const std::string &name, shared_ptr<DataCube> c) {
  lock_guard<boost::mutex> lock (mapMutex);
  cubeMap[name] = c;
}

boost::shared_ptr<vector<string> >
CubeManager::list_cubes() {
  lock_guard<boost::mutex> lock (mapMutex);

  size_t numCubes = cubeMap.size();
  boost::shared_ptr<vector<string> > cubeList(new vector<string>(numCubes));
  
  map<string, shared_ptr<DataCube> >::iterator it;
  for (it=cubeMap.begin() ; it != cubeMap.end(); it++ ) {
    cubeList->push_back(it->first);
  }
  return cubeList;

}
