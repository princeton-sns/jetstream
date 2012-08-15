#ifndef _dataplaneoperatorloader_H_
#define _dataplaneoperatorloader_H_


#include <sys/types.h>
#include <map>
#include <string>

#include "js_utils.h"
#include "dataplaneoperator.h"
#include "jetstream_types.pb.h"
#include "jetstream_dataplane.pb.h"

namespace jetstream {

  /**
   * @brief Loads operators from dynamic libraries
   *
   * The library will load an operator that is a child of DataPlaneOperator
   * from a dynamic library. It allows you then to instantiate new Instances of that
   * operator.
   */
class DataPlaneOperatorLoader {
 private:
   std::map<std::string, void *> cache;
   std::string path;

 public:

   /**
    * @brief New Instance with default path set to current dir.
    */
  DataPlaneOperatorLoader() : cache(), path("") {}

  /**
   * @brief Constructor
   *
   * @param default_path the default path to find the dynamic lib files.
   */
  DataPlaneOperatorLoader(std::string default_path) : cache(), path(default_path) {}
  ~DataPlaneOperatorLoader() {};

  
  /**
   * @brief Load an operator using the default naming scheme for the dynamic library
   *
   * @param name name of the operator
   */
  bool load(std::string name);


  /**
   * @brief 
   *
   * @param name of the operator
   * @param filename of the dynamic library
   */
  bool load(std::string name, std::string filename);
  
  /**
   * @brief 
   *
   * @param name of the operator
   * @param filename of the dynamic library
   * @param path to the dynamic library
   */
  bool load(std::string name, std::string filename, std::string path);
 

  /**
   * @brief get the default filename for the dynamic library
   *
   * The dynamic library should be named libNAME_operator.OSEXTENSION
   * 
   * @param name of the operator
   *
   * @return the filename
   */
  std::string get_default_filename(std::string name);


  /**
   * @brief Unload the dynamic library. 
   *
   * Have to make sure that there are no references to it, otherwise can get segfault.
   *
   * @param name of the operator
   */
  bool unload(std::string name);


  /**
   * @brief Create a new instance of the operator
   *
   * @param name of the operator
   *
   * @return new instance of the operator
   */
  DataPlaneOperator *newOp(std::string name);
};
}

#endif /* _dataplaneoperator_H_ */
