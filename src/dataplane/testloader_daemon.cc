#include <boost/format.hpp>
#include <boost/algorithm/string.hpp>
#include "nodedataplane.h"
#include "dataplaneoperator.h"
#include "dataplaneoperatorloader.h"
#include <dlfcn.h>



using namespace jetstream;


int
main (int argc, char **argv)
{
  setprogname(argv[0]);


  DataPlaneOperator *op = new DataPlaneOperator;
  op->process(NULL);
  delete op;

  DataPlaneOperatorLoader *opl = new DataPlaneOperatorLoader;
  opl->load("test");
  op = opl->newOp("test");
  op->process(NULL);
  delete op;
  opl->unload("test");


  /*opl->load("test", "/tmp/libtest_operator1.dylib");
  op = opl->newOp("test");
  op->execute();
  delete op;*/

  std::cout << "end" << std::endl;

  exit(0);
}
