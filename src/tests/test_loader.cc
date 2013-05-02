#include <boost/format.hpp>
#include <boost/algorithm/string.hpp>
#include "node.h"
#include "dataplane_operator_loader.h"
#include <dlfcn.h>

#include <gtest/gtest.h>

using namespace jetstream;

TEST(OperatorLoader, LoadAndUnloadWithPath)
{
  DataPlaneOperatorLoader *opl = new DataPlaneOperatorLoader();
  bool succ = opl->load("test", "src/dataplane/" + DataPlaneOperatorLoader::get_default_filename("test"));
  ASSERT_TRUE(succ);
  COperator *op = opl->newOp("test");
  std::vector< boost::shared_ptr<Tuple> > v;
  DataplaneMessage dummy;
  op->process(NULL, v, dummy);
  delete op;
  opl->unload("test");
  SUCCEED();
}

