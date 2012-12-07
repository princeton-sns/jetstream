
#include <iostream>

#include "experiment_operators.h"

#include "js_utils.h"
#include "queue_congestion_mon.h"

#include <gtest/gtest.h>

using namespace jetstream;
using namespace boost;
using namespace ::std;

TEST(CongestMon, QueueMon) {
  int TOTAL_ELEMS = 10;
  int elements[TOTAL_ELEMS];
  QueueCongestionMonitor mon(TOTAL_ELEMS, "test");
  
  for(int i=0; i < TOTAL_ELEMS; ++i) {
    mon.report_insert(elements + i, 1);
  }
  cout << "did inserts" <<endl;
  for(int i=0; i < TOTAL_ELEMS/2; ++i) {
    mon.report_delete(elements + i, 1);
  }
  
//  cout << "did removes" <<endl;
  
  js_usleep(1000 * 200);
  cout << "getting ratio" <<endl;
  
  ASSERT_EQ(mon.queue_length(), TOTAL_ELEMS/2);
  
  double lev = mon.capacity_ratio();
  cout << "congestion ratio: " << lev << endl;
/**
 '1' is the right answer here. Given that the queue is half-empty, if the insert
 rate remains the same then the queue will fill up.
 */
   ASSERT_DOUBLE_EQ(1, lev);


}