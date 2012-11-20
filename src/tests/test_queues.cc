
#include <iostream>

#include "experiment_operators.h"

#include "js_utils.h"
#include "queue_congestion_mon.h"

#include <gtest/gtest.h>

using namespace jetstream;
using namespace boost;
using namespace ::std;

const int compID = 4;


TEST(CongestMon, QueueMon) {
  int TOTAL_ELEMS = 10;
  int elements[TOTAL_ELEMS];
  QueueCongestionMonitor mon(TOTAL_ELEMS);
  
  for(int i=0; i < TOTAL_ELEMS; ++i) {
    mon.report_insert(elements + i);
  }
  cout << "did inserts" <<endl;
  for(int i=0; i < TOTAL_ELEMS/2; ++i) {
    mon.report_delete(elements + i);
  }
  
//  cout << "did removes" <<endl;
  
  js_usleep(1000 * 200);
  cout << "getting ratio" <<endl;
  
  double lev = mon.capacity_ratio();
  ASSERT_DOUBLE_EQ(0.5, lev);
  cout << "congestion ratio: " << lev << endl;


}