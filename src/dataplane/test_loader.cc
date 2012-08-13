#include <boost/format.hpp>
#include <boost/algorithm/string.hpp>
#include "nodedataplane.h"
#include "dataplaneoperator.h"
#include "dataplaneoperatorloader.h"
#include <dlfcn.h>

#include <gtest/gtest.h>

using namespace jetstream;

TEST(Operator, BaseOp)
{
  DataPlaneOperator *op = new DataPlaneOperator;
  op->process(NULL);
  delete op;
}

TEST(OperatorLoader, LoadAndUnloadWithPath)
{
  setprogname("test loader");


  DataPlaneOperatorLoader *opl = new DataPlaneOperatorLoader;
  opl->load("test", "src/dataplane/libtest_operator.dylib");
  DataPlaneOperator * op = opl->newOp("test");
  op->process(NULL);
  delete op;
  opl->unload("test");
  SUCCEED();
}

