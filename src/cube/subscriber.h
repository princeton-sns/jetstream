#ifndef SUBSCRIBER_2FAEJ0UJ
#define SUBSCRIBER_2FAEJ0UJ

#include "dataplaneoperator.h"
#include "jetstream_types.pb.h"

namespace jetstream {
namespace cube {
  
class Subscriber: public jetstream::DataPlaneOperator {
public:
  enum Action {NO_SEND, SEND, SEND_NO_BATCH, SEND_UPDATE} ;

  Subscriber (): DataPlaneOperator() {};
  virtual ~Subscriber ();

  //TODO
  bool has_cube() {return true;};

  virtual void process (boost::shared_ptr<jetstream::Tuple> t);
  virtual Action action_on_tuple(boost::shared_ptr<const jetstream::Tuple> const update) = 0;

  virtual void send(boost::shared_ptr<jetstream::Tuple> const new_value) = 0;
  virtual void send_update(boost::shared_ptr<jetstream::Tuple> const old_value, boost::shared_ptr<jetstream::Tuple> const new_value) = 0;

private:

};

} /* cube */
} /* jetsream */

#endif /* end of include guard: SUBSCRIBER_2FAEJ0UJ */
