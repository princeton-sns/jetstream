//  Copyright (c) 2012 Princeton University. All rights reserved.
//

#include <iostream>
#include <gtest/gtest.h>

using namespace ::std;

int main(int argc, char * argv[])
{
  cout << "Starting unit tests!\n";
  
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

