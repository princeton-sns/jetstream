
#include <iostream>

#include "experiment_operators.h"

#include "js_utils.h"
#include "queue_congestion_mon.h"
#include "window_congest_mon.h"


#include <gtest/gtest.h>

using namespace jetstream;
using namespace boost;
using namespace ::std;

TEST(CongestMon, QueueMon) {
  const int TOTAL_ELEMS = 10;
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


TEST(CongestMon, WindowLen) {
    WindowCongestionMonitor mon("testing monitor");
  
    for (int i =0; i < 100; ++i) {
      mon.report_insert(0, 1);
    }
    js_usleep(50 * 1000);
    mon.end_of_window(250, mon.get_window_start());
    mon.new_window_start();
    double cap_ratio = mon.capacity_ratio();
    ASSERT_LE(4.8, cap_ratio);
    ASSERT_GE(5.2, cap_ratio);
    mon.end_of_window(250, mon.get_window_start());
    ASSERT_EQ(10, mon.capacity_ratio()); //allow ramp-up if no data in window
}

TEST(CongestMon, SmoothQueueMon) {
  
  const int TOTAL_ELEMS = 100;
  SmoothingQCongestionMonitor mon(TOTAL_ELEMS, "test", 0);
  
  mon.report_insert(NULL, 50);
  
  double c_ratio = mon.capacity_ratio();
  ASSERT_EQ(0.5, c_ratio);

  mon.report_delete(NULL, 25);
  c_ratio = mon.capacity_ratio();
  ASSERT_EQ(1.25, c_ratio);

  mon.report_delete(NULL, 25);
  c_ratio = mon.capacity_ratio();
    //50 inserts, 50 deletes, queue size empty.
    //To fill the queue, we need an extra 25 inserts per period.
    //this is triple the 12.5 per period we currently have.
  ASSERT_EQ(INFINITY, c_ratio);

  
}
