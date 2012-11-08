#include "js_counting_executor.h"
#include "js_utils.h"
#include <boost/thread/thread.hpp>
#include <gtest/gtest.h>
#include <iostream>



using namespace jetstream;
using namespace std;

void tfunc(size_t sec) {

  cout << "Start Exec" <<endl;
  boost::this_thread::sleep(boost::posix_time::seconds(sec));
  cout << "End Exec" <<endl;

}

TEST(Executor, CountingExecutor) {

  CountingExecutor exec(1);
  exec.submit(boost::bind(&tfunc, 1));
  exec.submit(boost::bind(&tfunc, 3));
 
  ASSERT_EQ(2U, exec.outstanding_tasks());
  boost::this_thread::sleep(boost::posix_time::seconds(2));
  ASSERT_EQ(1U, exec.outstanding_tasks());
  boost::this_thread::sleep(boost::posix_time::seconds(3));
  ASSERT_EQ(0U, exec.outstanding_tasks());
}
