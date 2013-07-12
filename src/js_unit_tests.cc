//  Copyright (c) 2012 Princeton University. All rights reserved.
//

#include <iostream>
#define GTEST_HAS_TR1_TUPLE 0
#include <gtest/gtest.h>
#include <glog/logging.h>

using namespace ::std;

int main(int argc, char * argv[])
{
  // Create logger first thing
  google::LogToStderr();
  google::InitGoogleLogging(argv[0]);
  LOG(INFO) << "Starting unit tests!\n";

    
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

