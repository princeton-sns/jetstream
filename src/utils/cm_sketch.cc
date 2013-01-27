
#include "cm_sketch.h"
#include <stdlib.h>
#include <boost/random/mersenne_twister.hpp>
#include <boost/random/uniform_int.hpp>
#include <iostream>
#include <string.h>

using namespace ::std;

namespace jetstream {

uint32_t
CMSketch::hash(int hashid, int val) {
  uint64_t r = 0;
  r =  hashes[hashid].a * val + hashes[hashid].b;
  return (int) ( r % width);
}

void
CMSketch::add_item_h(uint32_t data_as_int, int new_val) {
  total_count += new_val;
  for(unsigned int i =0; i < depth; ++i) {
    val(hash(i, data_as_int), i )  += new_val;
  }
}


void
CMSketch::add_item(char* data, int data_len, int new_val) {
  uint32_t d =  jenkins_one_at_a_time_hash(data, data_len);  //TODO
  add_item_h(d, new_val);
}

count_val_t
CMSketch::estimate_h(uint32_t data_as_int) {
  uint32_t m = numeric_limits<uint32_t>::max();
  for(unsigned int i =0; i < depth; ++i) {
    uint32_t v = val(hash(i, data_as_int), i);
    m = m < v ? m : v;
  }
  return m;
}

count_val_t
CMSketch::estimate(char * data, int data_len) {
  uint32_t data_as_int = jenkins_one_at_a_time_hash(data, data_len);
  return estimate_h(data_as_int);
}

void
CMSketch::init(size_t w, size_t d, int rand_seed) {
  matrix = new u_int32_t[w * d];
  memset(matrix, 0, w * d * sizeof(int));
  hashes = new hash_t[d];
  width = w;
  depth = d;
  total_count = 0;


  boost::mt19937 gen;
  uint32_t bound = (1U << 31) -1;
  boost::random::uniform_int_distribution<> randsrc(1, bound);
  
  for(uint32_t i = 0; i < d; ++ i) {
    hashes[i].a = randsrc(gen);
    hashes[i].b = randsrc(gen);
  }
}


CMSketch::~CMSketch() {
  delete[] hashes;
  delete[] matrix;
}


CMMultiSketch::CMMultiSketch(size_t w, size_t d, int rand_seed) {

  exact_counts = new count_val_t*[EXACT_LEVELS];
  
  for(int i =0; i < EXACT_LEVELS; ++i) {
    int sz = 1 << ((EXACT_LEVELS- i) * BITS_PER_LEVEL);
    exact_counts[i] = new count_val_t[ sz];

    memset(exact_counts[i], 0, sz * sizeof(int));
  }
  panes = new CMSketch[LEVELS];
  for(int i =0; i < LEVELS; ++i) {
    panes[i].init(w, d, rand_seed);
  }
}


CMMultiSketch::~CMMultiSketch() {
  delete[] panes;
  for(int i =0; i < EXACT_LEVELS; ++i)
    delete[] exact_counts[i];
  delete[] exact_counts;
}

void
CMMultiSketch::add_item(char* data, int data_len, count_val_t new_val) {
  
  uint32_t data_as_int = jenkins_one_at_a_time_hash(data, data_len);
  add_item_h(data_as_int, new_val);
}

void
CMMultiSketch::add_item_h(uint32_t data_as_int, count_val_t new_val) {
  
  for (int i =0; i < LEVELS; ++i) {
//    cout << "inserting "<<data_as_int << " at level " << i<< endl;
    panes[i].add_item_h(data_as_int, new_val);
    data_as_int >>= BITS_PER_LEVEL;
  }
  for( int i=0; i < EXACT_LEVELS; ++i) {
    assert (data_as_int <  (1 << ((EXACT_LEVELS- i) * BITS_PER_LEVEL)));
    exact_counts[i][data_as_int] += new_val;
    data_as_int >>= BITS_PER_LEVEL;
  }

}


count_val_t
CMMultiSketch::contrib_from_level(int level, uint32_t dyad_start) {
  //dyad_start should be trimmed to at most (32 - BITS_PER_LEVEL * level) non-zero bits.


  count_val_t r;
  if (level >= LEVELS) {
    assert (dyad_start <  (1 << (EXACT_LEVELS- level) * BITS_PER_LEVEL));
    r = exact_counts[level - LEVELS][dyad_start];
  } else
    r = panes[level].estimate_h(dyad_start);
  
/*  uint32_t rng_start = dyad_start <<  (level * BITS_PER_LEVEL);
  uint32_t rng_end = rng_start+ (1 << level * BITS_PER_LEVEL) ;
  cout << "adding range [" << rng_start<< ", " << rng_end << ") at level " << level
     << " = " << r << endl;*/
  
  return r;
}

count_val_t
CMMultiSketch::range(char * lower, size_t l_size, char* upper, size_t u_size) {
  uint32_t l_as_int = jenkins_one_at_a_time_hash(lower, l_size);
  uint32_t u_as_int = jenkins_one_at_a_time_hash(upper, u_size);
  return hash_range(l_as_int, u_as_int);
}


count_val_t
CMMultiSketch::hash_range(uint32_t lower, uint32_t upper) {
//The model is that we work up the hierarchy, at each time trimming off the ends
// and then moving up.
  count_val_t sum = 0;
  for (int level = 0; level < LEVELS + EXACT_LEVELS ; ++level) {
//    uint32_t true_lower = (lower<< level *BITS_PER_LEVEL);
//    uint32_t true_upper = (upper<< level *BITS_PER_LEVEL);
//    cout << "range query [" <<  true_lower  << "," << true_upper<<"] at level "<< level << "\n";

    uint64_t next_level_start = lower & (~(0UL) << BITS_PER_LEVEL );
    if (lower != next_level_start)  //round up to next power of two
      next_level_start += (1 << BITS_PER_LEVEL);

    uint64_t next_level_end = (upper & (~(0UL) << BITS_PER_LEVEL )); //this endpoint is NOT included in next level
                //note that upper & mask can be zero, meaning there's no power of two before this range
    
    if (next_level_start < next_level_end) {
  //    cout << " next level will be [" << next_level_start << ", "<< next_level_end <<")\n";
      assert ( (next_level_start - lower) <  (1 << (BITS_PER_LEVEL )));
      
      for (uint64_t j = lower; j < next_level_start; j++)
        sum += contrib_from_level(level, j);
      
      assert ( (upper - next_level_end) <  (1 << (BITS_PER_LEVEL )));
      
      for (uint64_t j = next_level_end ; j <= upper; ++j)
        sum += contrib_from_level(level, j);
    } else {
//      cout << " last level; [" << lower << "-" << upper << ")\n";
      assert ( (upper - lower) <  (1 << (BITS_PER_LEVEL + 1)));
      for(uint64_t j =lower; j <= upper; ++j)
        sum += contrib_from_level(level, j);
      break;
    }
    
    lower = next_level_start >> BITS_PER_LEVEL;
    upper = (next_level_end-1) >> BITS_PER_LEVEL ;

  }
  if (sum > panes[0].total_count)
    return panes[0].total_count;
  else
    return sum;
}


count_val_t
CMMultiSketch::quantile(float quantile) {
  count_val_t target_sum = panes[0].total_count * quantile;
  
  count_val_t bisect_low = 0, bisect_hi = numeric_limits<uint32_t>::max();
  cout << "for quantile " <<quantile << "looking for a hash range that adds up to " << target_sum << endl;
    //do a search over the interval 0 - quantile
  
  int iters = 0;
  while(bisect_hi > bisect_low + 1) {
    count_val_t mid = bisect_hi / 2 + bisect_low / 2;
    count_val_t range_sum = hash_range(0, mid);
//    std::cout << iters<< " scanning [0-" << mid << "]; sum was " << range_sum<< std::endl;
    if (range_sum >= target_sum) //guessed too high, lower upper bound
      bisect_hi = mid;
    else
      bisect_low = mid;
    
    if (iters ++ > 40)
      break;
  }
  count_val_t bound_from_left = bisect_hi;
  cout << "finished approach from left; got " << bound_from_left << endl;
  
  
  iters = 0;
  target_sum = panes[0].total_count * (1 - quantile);
  bisect_hi = numeric_limits<uint32_t>::max() - 1;
  bisect_low = 0;
//  cout << "searching from right for " << target_sum << endl;
  while(bisect_hi > bisect_low + 1) { //now repeat, but searching the right-side of the range
    count_val_t mid = bisect_hi / 2 + bisect_low / 2;
    count_val_t range_sum = hash_range(mid, numeric_limits<uint32_t>::max());
//    std::cout << iters<< " scanning " << mid << "-max, sum was " << range_sum<< std::endl;

    if (range_sum < target_sum) //guessed too high a lower bound
      bisect_hi = mid;
    else
      bisect_low = mid;
    
    if (iters ++ > 40)
      break;    
  }
  cout << "scanning from right, got " << bisect_hi << endl;
  return (bound_from_left+ bisect_hi)/2;
   //return bound_from_left;
}

}
