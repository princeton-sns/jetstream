#include <boost/thread/locks.hpp>
#include "cube_manager.h"

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
CubeManager::create_cube (const std::string &name, const CubeSchema &schema) 
{
  //TODO: The cube constructor does several things, some of which may fail; we
  //need it to throw an exception in case of failure, which should be caught here
  shared_ptr<DataCube> c (new cube::MysqlCube(schema));
  put_cube(name, c);
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
CubeManager::put_cube (const std::string &name, shared_ptr<DataCube> c)
{
  lock_guard<boost::mutex> lock (mapMutex);
  cubeMap.insert (std::pair<std::string, shared_ptr<DataCube> > (name, c));
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