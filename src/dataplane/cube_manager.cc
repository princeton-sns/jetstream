#include <boost/thread/locks.hpp>
#include "mysql_cube.h"

#ifndef NO_MASSTREE
#include "mt_cube.h"
#endif 
#include "cube_manager.h"
#include <boost/regex.hpp>


#include <glog/logging.h>

using boost::shared_ptr;
using namespace jetstream;
using namespace ::std;


CubeManager::CubeManager (const NodeConfig &conf): config(conf) {
  cube::MysqlCube::set_db_params(config.cube_db_host, config.cube_db_user, config.cube_db_pass, config.cube_db_name);
}


boost::shared_ptr<DataCube>
CubeManager::get_cube (const std::string &name)
{
  boost::lock_guard<boost::mutex> lock (mapMutex);
  if (cubeMap.count(name) > 0)
    return cubeMap[name];
  else {
    shared_ptr<DataCube> c;
    return c;
  }
}

boost::shared_ptr<DataCube>
CubeManager::create_cube ( const std::string &name,
                           const CubeSchema &schema,
                           bool overwrite_if_present) {

  static const boost::regex NAME_PAT("[a-zA-Z0-9_]+$");
  boost::lock_guard<boost::mutex> lock (mapMutex);

  shared_ptr<DataCube> c;
  if (!regex_match(name, NAME_PAT)) {
    LOG(WARNING) << "Invalid cube name caught in dataplane:" <<
          " should be a valid C identifier but got " << name;
    return c;
  }


  std::map<string, shared_ptr<DataCube> >::iterator iter;
  iter = cubeMap.find(name);
  if (iter != cubeMap.end()) { //cube already exists, so return it
    if (!iter->second)
      LOG(WARNING) << "Cube " << name << " had null entry in table";
    c = iter->second;
    if (overwrite_if_present) {
      c->clear_contents();
    }
    return c;
  }

  //TODO: The cube constructor does several things, some of which may fail; we
  //need it to throw an exception in case of failure, which should be caught here
  
#ifndef NO_MASSTREE  
  if (schema.has_impl() && schema.impl() == CubeSchema::Masstree)
//  if (MASSTREE_CUBE)
    c = shared_ptr<DataCube>(new cube::MasstreeCube(schema, name, overwrite_if_present, config));
  else
#endif
    c = shared_ptr<DataCube>(new cube::MysqlCube(schema, name, overwrite_if_present, config));
//  set_batch(10)
  c->create();
  cubeMap[name] = c;
  return c;
}

void
CubeManager::destroy_cube (const std::string &name)
{
  boost::lock_guard<boost::mutex> lock (mapMutex);
  cubeMap[name]->mark_as_deleted();
  cubeMap.erase(name);
}


void
CubeManager::put_cube (const std::string &name, shared_ptr<DataCube> c) {
  boost::lock_guard<boost::mutex> lock (mapMutex);
  LOG_IF(FATAL, c == NULL) << "should not insert a null cube";
  cubeMap[name] = c;
}

boost::shared_ptr<vector<string> >
CubeManager::list_cubes() {
  boost::lock_guard<boost::mutex> lock (mapMutex);

  size_t numCubes = cubeMap.size();
  boost::shared_ptr<vector<string> > cubeList(new vector<string>(numCubes));

  map<string, shared_ptr<DataCube> >::iterator it;
  for (it=cubeMap.begin() ; it != cubeMap.end(); it++ ) {
    cubeList->push_back(it->first);
  }
  return cubeList;

}
