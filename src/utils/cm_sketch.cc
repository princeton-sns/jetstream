
#include "cm_sketch.h"
#include <stdlib.h>
#include <boost/random/mersenne_twister.hpp>
#include <boost/random/uniform_int.hpp>


namespace jetstream {

int
CMSketch::hash(int hashid, int val) {
  uint64_t r = 0;
  r =  hashes[hashid].a * val + hashes[hashid].b;
  return (int) ( r % width);
}


void
CMSketch::add_item(char* data, int data_len, int new_val) {
  total_count += new_val;
  for(int i =0; i < depth; ++i) {
    int data_as_int = jenkins_one_at_a_time_hash(data, data_len);  //TODO
    val(hash(i, data_as_int), i )  += new_val;
  }

}

uint32_t CMSketch::estimate(char * data, int data_len) {
  uint32_t m = UINT32_MAX;
  for(int i =0; i < depth; ++i) {
    int data_as_int = jenkins_one_at_a_time_hash(data, data_len);
    uint32_t v = val(hash(i, data_as_int), i);
    m = m < v ? m : v;
  }
  return m;
}

void
CMSketch::init(size_t w, size_t d, int rand_seed) {
  matrix = new u_int32_t[w * d];
  hashes = new hash_t[d];
  width = w;
  depth = d;
  total_count = 0;


  boost::mt19937 gen;
  boost::random::uniform_int_distribution<> randsrc(1,1<<31 -1);
  
  for(int i = 0; i < d; ++ i) {
    hashes[i].a = randsrc(gen);
    hashes[i].b = randsrc(gen);
  }
}


CMSketch::~CMSketch() {
  delete hashes;
  delete matrix;
}


CMMultiSketch::CMMultiSketch(size_t l, size_t w, size_t d, int rand_seed) :
  levels(l) {
  panes = new CMSketch[levels];
  for(int i =0; i < levels; ++i) {
    panes->init(w, d, rand_seed);
  }
}


CMMultiSketch::~CMMultiSketch() {
  delete panes;
}

void
CMMultiSketch::add_item(char* data, int data_len, int new_val) {

}

uint32_t
CMMultiSketch::quantile(float quantile) {

}

}