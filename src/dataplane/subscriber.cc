#include "subscriber.h"
#include <glog/logging.h>

using namespace std;
using namespace jetstream::cube;

void Subscriber::process (boost::shared_ptr<jetstream::Tuple> t) {
  LOG(FATAL)<<"Cube Subscriber should never process";
}

const string Subscriber::my_type_name("Subscriber");
