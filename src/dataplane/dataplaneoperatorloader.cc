#include "dataplaneoperatorloader.h"
#include "dataplaneoperator.h"
#include <iostream>
#include <dlfcn.h>

using namespace std;

jetstream::DataPlaneOperatorLoader::~DataPlaneOperatorLoader() 
{

}



void jetstream::DataPlaneOperatorLoader::load(string name)
{
  string filename = "lib"+name+"_operator.dylib";
  this->load(name, filename);
}

void jetstream::DataPlaneOperatorLoader::load(string name, string filename)
{
  void *dl_handle = dlopen(filename.c_str(), RTLD_NOW);
  if(dl_handle == NULL)
  {
    std::cerr << dlerror() << std::endl;
    exit(-1);
  }

  this->cache[name] = dl_handle;
}

void jetstream::DataPlaneOperatorLoader::unload(string name)
{
  void *dl_handle = this->cache[name];
  dlclose(dl_handle);
  this->cache.erase(name);
}

jetstream::DataPlaneOperator *jetstream::DataPlaneOperatorLoader::newOp(string name)
{
  void *dl_handle = this->cache[name];
  maker_t *mkr = (maker_t *) dlsym(dl_handle, "maker");
  jetstream::DataPlaneOperator *dop = mkr();
  return dop;
}
