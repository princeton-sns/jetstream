//
//  cm_sketch.h
//  JetStream
//
//  Created by Ariel Rabkin on 1/24/13.
//  Copyright (c) 2013 Ariel Rabkin. All rights reserved.
//

#ifndef JetStream_cm_sketch_h
#define JetStream_cm_sketch_h

#include "js_defs.h"

namespace jetstream {

struct hash_t {
  int a, b;
};

class CMSketch {
 friend class CMMultiSketch;
 u_int32_t * matrix;
 hash_t * hashes;
 size_t width;
 size_t depth;
 uint32_t total_count;
 
 public:
  void add_item(char* data, int data_len, int new_val); //add new_val, associated with data
  uint32_t estimate(char * data, int data_len);
  
 CMSketch(): matrix(0), hashes(0),  width(0), total_count(0) {} //levels(0),

 CMSketch(size_t w, size_t d, int rand_seed) { init(w, d, rand_seed); }
 
 ~CMSketch();
 
 protected:
  int hash(int hashid, int val); //returns a value in [0, width-1]
  u_int32_t& val(size_t w, size_t d) {
//    assert (level < levels);
    assert (w < width);
    assert (d < depth);
    return matrix[w * depth + d];// level * (width*depth) + 
  }

 private:
  void init(size_t w, size_t d, int rand_seed);
 
  void operator= (const CMSketch &)
    { assert(false); } //LOG(FATAL) << "cannot copy a CMSketch"; }
  CMSketch (const CMSketch &) 
    { assert(false); } // LOG(FATAL) << "cannot copy a CMSketch"; }

};


class CMMultiSketch {

 private:
  size_t levels;
  CMSketch * panes;

 public:
  CMMultiSketch(size_t l, size_t w, size_t d, int rand_seed);
  ~CMMultiSketch();

  void add_item(char* data, int data_len, int new_val); //add new_val, associated with data
  uint32_t estimate(char * data, int data_len);
  uint32_t rangequery(


  uint32_t quantile(float quantile);

 private:
  void operator= (const CMMultiSketch &) 
    { assert(false); } //LOG(FATAL) << "cannot copy a CMSketch"; }
  CMMultiSketch (const CMMultiSketch &) 
    { assert(false); } // LOG(FATAL) << "cannot copy a CMSketch"; }


};

}


#endif
