#include <boost/thread/locks.hpp>
#include "cube_manager.h"

using namespace boost;
using namespace jetstream;

shared_ptr<DataCube> 
CubeManager::get_cube (const std::string &name) 
{
  lock_guard<boost::mutex> lock (mapMutex);
  return cubeMap[name];
}
  
shared_ptr<DataCube> 
CubeManager::create_cube (const std::string &name, const CubeSchema &schema) 
{
  shared_ptr<DataCube> c(new cube::MysqlCube(schema));
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

