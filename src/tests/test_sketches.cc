#include "cm_sketch.h"

#include <gtest/gtest.h>


using namespace ::std;
using namespace jetstream;


TEST(CMSketch, AddAndQuery) {

  CMSketch c(10, 100, 2);
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


}