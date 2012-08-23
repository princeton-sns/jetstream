#ifndef JetStream_cube_h
#define JetStream_cube_h

#include <iterator>
#include <vector>
#include <boost/shared_ptr.hpp>
#include "dataplaneoperator.h"  //needed only for Receiver


#include "jetstream_types.pb.h"


namespace jetstream {

using namespace ::std;
using namespace boost;
//FIXME: Can we use Boost types here?

/**
*  A class to represent a cube in memory. 
*/
class DataCube : public Receiver {
  
public:
  virtual void process(boost::shared_ptr<Tuple> t) {} //inserts a tuple
  
  DataCube(std::string _schema):schema(_schema) {}
  virtual ~DataCube() {;}
  
  /**
  * It's possible to mark a cube as locked. The intended use of this is to allow
  * graceful deletion. The deleter marks the cube as frozen. As updates to the cube fail,
  * data sources drop their pointer. When the last smart pointer is removed,
  * the cube is deleted. 
  *
  * Possibly a different mechanism is needed to do visibility control.  
  *
  */
  void mark_as_deleted() {is_frozen = true; }

  //iterator<forward_iterator_tag, Tuple> stream_tuples(Tuple k);
  

//TODO: should have an entry here for the aggregation/update function.
  
private:
  string schema;
  bool is_frozen;
//TODO should figure out how to implement this
  
};

}

#endif
