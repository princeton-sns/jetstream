//  Copyright (c) 2012 Princeton University. All rights reserved.
//

#include <iostream>
#include <gtest/gtest.h>
#include <glog/logging.h>

using namespace ::std;

int main(int argc, char * argv[])
{
  // Create logger first thing
  google::LogToStderr();
  google::InitGoogleLogging(getprogname());
  LOG(INFO) << "Starting unit tests!\n";

    
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

