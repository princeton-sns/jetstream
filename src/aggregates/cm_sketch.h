//
//  cm_sketch.h
//  JetStream
//
//  Created by Ariel Rabkin on 1/24/13.
//  Copyright (c) 2013 Ariel Rabkin. All rights reserved.
//

#ifndef JetStream_cm_sketch_h
#define JetStream_cm_sketch_h

#include <stdint.h>
#include "js_defs.h"
#include "quantile_est.h"

namespace jetstream {

struct hash_t {
  int a, b;
};
 
const int32_t MAX_H_VAL = INT_MAX; //max OUTPUT from hash

class CMSketch {
 friend class CMMultiSketch;
 count_val_t * matrix;
 hash_t * hashes;
 size_t width; //in elements, not in log-format
 size_t width_bitmask;
 size_t depth_;
 count_val_t total_count;
 
 public:
 
  void add_item_h(int data, int new_val);
  void add_item(char* data, size_t data_len, int new_val); //add new_val, associated with data

  count_val_t estimate_h(int data);
  count_val_t estimate(char * data, size_t data_len);

 CMSketch(): matrix(0), hashes(0),  width_bitmask(0), total_count(0) {} //levels(0),

 CMSketch(size_t w, size_t d, int rand_seed) { init(w, d, rand_seed); }
 
 ~CMSketch();
 
  size_t size(); //size in bytes
  
  size_t depth() {return depth_;}
  
  bool can_accept(const CMSketch & rhs);
  
  bool merge_in(const CMSketch & rhs);
 
 protected:
  inline uint32_t hash(int hashid, int hash_in_val); //returns a value in [0, width-1]
  
  count_val_t& val(size_t w, size_t d) {
//    assert (level < levels);
    assert (w < width);
    assert (d < depth_);
    return matrix[d * width + w];// level * (width*depth) +
  }
  
  size_t matrix_bytes() {
    return depth_ * width * sizeof(count_val_t);
  }

 private:
  void init(size_t w, size_t d, int rand_seed);
 
   void operator= (const CMSketch &)  
    { assert(false); } //LOG(FATAL) << "cannot copy a CMSketch"; }
  CMSketch (const CMSketch &) 
    { assert(false); } // LOG(FATAL) << "cannot copy a CMSketch"; }

};


/**
exact level 0 has 2^(EXACT_LEVELS * BITS_PER_LEVEL) ints
So if exact_levels =4, we have 2 ^ 10 bytes; for 5 exact levels, it'll be 4kb
meanwhile each pane is depth * (width + 2) ints
so if depth = 10 and width = 256, that's 10kb per pane
                              
*/
class CMMultiSketch: public QuantileEstimation {
  
  static const unsigned int BITS_PER_LEVEL = 2;
  static const unsigned int EXACT_LEVELS = 6;

  static const int LEVELS = (32 - BITS_PER_LEVEL * EXACT_LEVELS) / BITS_PER_LEVEL;
 private:
  CMSketch * panes;  //sorted from finest to coarsest. Panes[0] is the raw sketch
              //panes[1] is the dyadic ranges of length 2^BITS_PER_LEVEL.
  count_val_t ** exact_counts; //also finest to coarsest. Last entry has 2 ^ BITS_PER_LEVEL entries
  int rand_seed;
  void fillIn(const JSCMSketch&);


 public:
  CMMultiSketch(size_t w, size_t d, int random_seed);
  
  CMMultiSketch(const JSCMSketch& s) {fillIn(s);}
  CMMultiSketch(const JSSummary& s) {fillIn(s.sketch());}
  
  ~CMMultiSketch();

  virtual void add_item(int data, count_val_t new_val);

  count_val_t estimate_point(int data) const {
    return panes[0].estimate_h(data);
  }

  
  count_val_t contrib_from_level(int level, uint32_t dyad_start) const;

  count_val_t hash_range(unsigned lower, unsigned upper) const ;

/*
  count_val_t range(char * lower, size_t l_size, char* upper, size_t u_size);
  
  count_val_t estimate(char * data, size_t data_len) {
    return panes[0].estimate(data, data_len);
  }
  void add_item(char* data, size_t data_len, count_val_t new_val); //add new_val, associated with data
  */

  virtual int quantile(double quantile);
  
  virtual size_t size() const ;//size in bytes

  bool merge_in(const CMMultiSketch & rhs);

  size_t exact_l_size(size_t lev) const { //size in elements [not bytes]
    return (1U << ( (EXACT_LEVELS- lev) * BITS_PER_LEVEL));
  }
  
  virtual void serialize_to(JSSummary&) const;
  
  
 private:
  void operator= (const CMMultiSketch &) 
    { assert(false); } //LOG(FATAL) << "cannot copy a CMSketch"; }
  CMMultiSketch (const CMMultiSketch &) 
    { assert(false); } // LOG(FATAL) << "cannot copy a CMSketch"; }


};

}


#endif
