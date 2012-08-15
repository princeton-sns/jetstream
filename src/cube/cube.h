#ifndef JetStream_cube_h
#define JetStream_cube_h

#include <iterator>
#include <vector>
#include <boost/shared_ptr.hpp>



#include "jetstream_types.pb.h"


namespace jetstream {

using namespace ::std;
using namespace boost;
//FIXME: Can we use Boost types here?

/**
*  A class to represent a cube in memory. 
*/
class DataCube {
  
public:
  void process(const Tuple& t); //inserts a tuple

  //iterator<forward_iterator_tag, Tuple> stream_tuples(Tuple k);
  

//TODO: should have an entry here for the aggregation/update function.
  
private:
//TODO should figure out how to implement this
  
};

}

#endif
