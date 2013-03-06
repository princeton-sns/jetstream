#include "dataplane_operator_loader.h"
#include "dataplaneoperator.h"
#include "base_operators.h"
#include "experiment_operators.h"
#include "base_subscribers.h"
#include "filter_subscriber.h"
#include "latency_measure_subscriber.h"
#include "rand_source.h"
#include "variable_sampling.h"
#include "topk_tput.h"
#include "summary_operators.h"

#include <iostream>
#include <dlfcn.h>

using namespace std;

std::string jetstream::DataPlaneOperatorLoader::get_default_filename(string name)
{
#ifdef __APPLE__
  return "lib"+name+"_operator.dylib";
#else
  return "lib"+name+"_operator.so";
#endif

}
bool jetstream::DataPlaneOperatorLoader::load(string name)
{
  return load(name, get_default_filename(name));
}

bool jetstream::DataPlaneOperatorLoader::load(string name, string filename)
{
   return load(name, filename, path);
}

bool jetstream::DataPlaneOperatorLoader::load(string name, string filename, string path)
{
  void *dl_handle = dlopen((path + filename).c_str(), RTLD_NOW);
  if(dl_handle == NULL)
  {
    std::cerr << dlerror() << std::endl;
    return false;
  }

  this->cache[name] = dl_handle;
  return true;
}

bool jetstream::DataPlaneOperatorLoader::unload(string name)
{
  if (cache.count(name) < 1)
    return false;
  void *dl_handle = this->cache[name];
  if (dlclose(dl_handle) == 0)
  {
    this->cache.erase(name);
    return true;
  }
  return false;
}

#define REGISTER_OP(x) if (name.compare(#x) == 0) return new x()

jetstream::DataPlaneOperator *jetstream::DataPlaneOperatorLoader::newOp(string name)
{
  //some special cases for internal operators
  
    // These are base operators in base_operators
  REGISTER_OP(FileRead);
  REGISTER_OP(StringGrep);
  REGISTER_OP(GenericParse);
  REGISTER_OP(ExtendOperator);
  REGISTER_OP(OrderingOperator);
  REGISTER_OP(SampleOperator);
  REGISTER_OP(HashSampleOperator);
  REGISTER_OP(TRoundingOperator);
  REGISTER_OP(UnixOperator);
  REGISTER_OP(TimestampOperator);
  REGISTER_OP(CSVParse);
  REGISTER_OP(CSVParseStrTk);
  REGISTER_OP(URLToDomain);
  REGISTER_OP(GreaterThan);

  //operators on quantiles
  REGISTER_OP(QuantileOperator);
  REGISTER_OP(ToSummary);
  REGISTER_OP(SummaryToCount);
  REGISTER_OP(DegradeSummary);

      // Experimental purposes 
  REGISTER_OP(DummyReceiver);
  REGISTER_OP(SendK);
  REGISTER_OP(ContinuousSendK);
  REGISTER_OP(RateRecordReceiver);
  REGISTER_OP(SerDeOverhead);
  REGISTER_OP(EchoOperator);
  REGISTER_OP(RandSourceOperator);
  REGISTER_OP(RandEvalOperator);
  REGISTER_OP(MockCongestion);
  REGISTER_OP(FixedRateQueue);
  REGISTER_OP(ExperimentTimeRewrite);
  REGISTER_OP(CountLogger);
  
    // Subscribers
  REGISTER_OP(TimeBasedSubscriber);
  REGISTER_OP(LatencyMeasureSubscriber);
  REGISTER_OP(OneShotSubscriber);
  REGISTER_OP(VariableCoarseningSubscriber);
  REGISTER_OP(FilterSubscriber);
  
   // Congestion response
  REGISTER_OP(VariableSamplingOperator);
  REGISTER_OP(CongestionController);
  
  // Multi-round topk
  REGISTER_OP(MultiRoundSender);
  REGISTER_OP(MultiRoundCoordinator);
  
  
  if(cache.count(name) < 1)
  {
    bool loaded = load(name);
    if (!loaded)
      return NULL;
  }
  void *dl_handle = this->cache[name];
  maker_t *mkr = (maker_t *) dlsym(dl_handle, "maker");
  if(mkr == NULL)
  {
    std::cerr << dlerror() << std::endl;
    return NULL;
  }

  jetstream::DataPlaneOperator *dop = mkr();
  return dop;
}
