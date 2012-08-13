//  Copyright (c) 2012 Princeton University. All rights reserved.
//

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

using namespace::std;
using namespace boost;

class CubeManager {
public:
  CubeManager();
  shared_ptr<DataCube> get_cube(string s) { return cubeDict[s]; }
  void put_cube(string s, shared_ptr<DataCube> c) {
       cubeDict.insert( pair<string, shared_ptr<DataCube> >(s, c) ); }
private:
  map<string, shared_ptr<DataCube> > cubeDict;
  
};

}

#endif
