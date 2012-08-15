#include "cube_manager.h"

#include <gtest/gtest.h>

using namespace jetstream;
using namespace boost;

TEST(CubeManager, MakeAndDestroy) {
  
  CubeManager mgr;
  shared_ptr<DataCube> cptr(new DataCube);
  mgr.put_cube("my cube", cptr);
  DataCube* c2 = mgr.get_cube("my cube").get();
  EXPECT_EQ(cptr.get(), c2);
}

