
#include "mt_cube.h"

using namespace ::std;

namespace jetstream {
namespace cube {


MasstreeCube::MasstreeCube (jetstream::CubeSchema const _schema,
               string _name,
               bool overwrite_if_present, const NodeConfig &conf)
      : DataCubeImpl(_schema, _name, conf){
  
  
}


}

}