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
  boost::shared_ptr<Tuple> t(new Tuple);
  op->process(t);
  delete op;
}

TEST(OperatorLoader, LoadAndUnloadWithPath)
{
  setprogname("test loader");


  DataPlaneOperatorLoader *opl = new DataPlaneOperatorLoader;
  opl->load("test", "src/dataplane/libtest_operator.dylib");
  DataPlaneOperator *op = opl->newOp("test");
  boost::shared_ptr<Tuple> t(new Tuple);
  op->process(t);
  delete op;
  opl->unload("test");
  SUCCEED();
}

