#ifndef JetStream_counter_h
#define JetStream_counter_h


#include <boost/thread.hpp>
#include "js_defs.h"

typedef uint64_t counter_t;

namespace jetstream {

class Counter {

  public:
    void update(counter_t i) {
      {
        boost::unique_lock<boost::mutex> lock(internals);
        counter += i;
      }
    }
  
    counter_t read() {
      {
        boost::unique_lock<boost::mutex> lock(internals);
        return counter;
      }
    }
  
    Counter(): counter(0) {}
  
  protected:
    mutable boost::mutex internals;
    counter_t counter;
};

}

#endif
