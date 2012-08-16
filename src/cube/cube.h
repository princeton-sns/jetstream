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

  //iterator<forward_iterator_tag, Tuple> stream_tuples(Tuple k);
  

//TODO: should have an entry here for the aggregation/update function.
  
private:
  string schema;
//TODO should figure out how to implement this
  
};

}

#endif
