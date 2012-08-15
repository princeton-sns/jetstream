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
* Responsible for loading operator code and creating operator objects.
*/
class DataPlaneOperatorLoader {
 private:
   std::map<std::string, void *> cache;

 public:
  DataPlaneOperatorLoader() : cache() {}
  ~DataPlaneOperatorLoader();
  void load(std::string name);
  void load(std::string name, std::string filename);
  void unload(std::string name);
  DataPlaneOperator *newOp(std::string name);
};
}

#endif /* _dataplaneoperator_H_ */
