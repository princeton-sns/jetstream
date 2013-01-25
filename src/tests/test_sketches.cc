#include "cm_sketch.h"

#include <gtest/gtest.h>


using namespace ::std;
using namespace jetstream;


TEST(CMSketch, AddAndQuery) {

  CMSketch c(20, 8, 2);
  int seq[] = {  2, 7, 4, 8, 12, 100};
  int seq_size = sizeof(seq) / sizeof(int);
  for (int i = 0; i <  seq_size; ++i) {
    c.add_item( reinterpret_cast<char *>(& (seq[i])), sizeof(int), 2 * i + 1);
  }
  for (int i = 0; i < seq_size; ++i) {
    int sval = c.estimate(reinterpret_cast<char *>(& (seq[i])), sizeof(int));
    cout << "got " << sval << " and expected " << 2 * i + 1 << endl;
    ASSERT_GE(sval, 2* i + 1);
  }

}


TEST(CMSketch, Quantile) {
  CMMultiSketch c(20, 10, 3);

//  int seq[100];
  for (int i = 0; i < 20; ++i) {
    c.add_item_h(i, 1);
  }
  
  int collisions = 0;
  for(int i = 0; i < 20; ++i) {
    int est = c.estimate_h(i);
//    cout << "estimate for " << i << " is " << est << endl;
    ASSERT_GE(est, 1);
    collisions += est - 1;
  }

  int r = c.hash_range(0, UINT32_MAX);
  cout << "maximal range has estimated sum: " << r << endl;

  r = c.hash_range(1000, UINT32_MAX);
  cout << "over-high range has estimated sum: " << r << endl;
  ASSERT_LE(r, collisions);

  
  r = c.hash_range(0, 1);
  cout << "range [0-1] has estimated sum: " << r << endl;
  ASSERT_GE(r, 2);
  
  r =  c.hash_range(4, 8);
  cout << "range [4-8] has estimated sum: " <<r << endl;
  ASSERT_GE(r, 5);
  ASSERT_LE(r, 5 + collisions);
  
  r =  c.hash_range(0, 10);
  cout << "range [0-10] has estimated sum: " <<r << endl;
  ASSERT_GE(r, 11);
  ASSERT_LE(r, 11 + collisions);
  
  r =  c.hash_range(3, 10);
  cout << "range [3-10] has estimated sum: " <<r << endl;
  ASSERT_GE(r, 8);
  ASSERT_LE(r, 8 + collisions);

  
  r =  c.hash_range(0, 10000);
  cout << "range [0-10000] has estimated sum: " <<r << endl;
  ASSERT_GE(r, 20);
  ASSERT_LE(r, 20+ collisions); //should be upper bounded by max
  
  for (int i = 30; i > 10; --i) {
    r =  c.hash_range(0, 1 << i);
    if (r < 20 ||  r > 20 + collisions) {
      cout << "for range 0 - 2 ^ "<< i << " ("<<  (1 << i)<< ")" << endl;
      ASSERT_GE(r, 20);
      ASSERT_LE(r, 20+ collisions); //should be upper bounded by max
    }
  }
  
  int quantile_pts[] = {10, 25, 50, 75, 90};
  int quantile_list_len = sizeof(quantile_pts) / sizeof(int);
  for (int i =0; i < quantile_list_len; ++i) {
    int q = c.quantile( quantile_pts[i] / 100.0) ;
    cout << quantile_pts[i]<<"th percentile is " << q << endl;
  }

}