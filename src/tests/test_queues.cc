
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
  QueueCongestionMonitor mon(TOTAL_ELEMS / 2);
  
  for(int i=0; i < TOTAL_ELEMS; ++i) {
    mon.report_insert(elements + i);
  }
  
  for(int i=0; i < TOTAL_ELEMS/2; ++i) {
    mon.report_delete(elements + i);
  }  
  
  js_usleep(1000 * 200);
  
  double lev = mon.capacity_ratio();
  cout << "congestion ratio: " << lev << endl;


}