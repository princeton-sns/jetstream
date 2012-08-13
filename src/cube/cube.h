//  Copyright (c) 2012 Princeton University. All rights reserved.
//



#ifndef JetStream_cube_h
#define JetStream_cube_h


#include "cube.h"

#include <iterator>

namespace jetstream {

using namespace ::std;
  
//FIXME: Can we use Boost types here?
class Tuple;
class Key;

/**
*  A class to represent a cube in memory. 
*/
class DataCube {
  
public:
  void insert_tuple(const Tuple& t);
  iterator<forward_iterator_tag, Tuple> stream_tuples(Key k);
  void process(Tuple t);
    

//TODO: should have an entry here for the aggregation/update function.
  
private:
//TODO should figure out how to implement this
  
};

}

#endif
