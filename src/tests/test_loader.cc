#include <boost/format.hpp>
#include <boost/algorithm/string.hpp>
#include "node.h"
#include "dataplaneoperator.h"
#include "dataplane_operator_loader.h"
#include <dlfcn.h>

#include <gtest/gtest.h>

using namespace jetstream;

TEST(Operator, BaseOp)
{
  DataPlaneOperator *op = new DataPlaneOperator;
  boost::shared_ptr<Tuple> t(new Tuple);
  op->process(t);
  delete op;
}

TEST(OperatorLoader, LoadAndUnloadWithPath)
{
  DataPlaneOperatorLoader *opl = new DataPlaneOperatorLoader;
  bool succ = opl->load("test", "src/dataplane/libtest_operator.dylib");
  ASSERT_TRUE(succ);
  DataPlaneOperator *op = opl->newOp("test");
  boost::shared_ptr<Tuple> t(new Tuple);
  op->process(t);
  delete op;
  opl->unload("test");
  SUCCEED();
}

