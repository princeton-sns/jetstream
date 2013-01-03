#ifndef JetStream_topk_tput_h
#define JetStream_topk_tput_h

#include <glog/logging.h>

#include "jetstream_types.pb.h"
#include "dataplaneoperator.h"
#include "cube.h"
#include "subscriber.h"


namespace jetstream {

enum ProtoState {
  ROUND_1,
  ROUND_2,
  ROUND_3
};

class MultiRoundSender: public jetstream::cube::Subscriber {

public:
    MultiRoundSender(): Subscriber (){};
    virtual ~MultiRoundSender() {};

    virtual Action action_on_tuple(boost::shared_ptr<const jetstream::Tuple> const update);

    virtual void post_insert(boost::shared_ptr<jetstream::Tuple> const &update,
                                 boost::shared_ptr<jetstream::Tuple> const &new_value);

    virtual void post_update(boost::shared_ptr<jetstream::Tuple> const &update,
                                 boost::shared_ptr<jetstream::Tuple> const &new_value,
                                 boost::shared_ptr<jetstream::Tuple> const &old_value);


    virtual void meta_from_downstream(const DataplaneMessage & msg);


private:
  

  GENERIC_CLNAME
};



class MultiRoundCoordinator: public DataPlaneOperator {

 private:
   std::vector<boost::shared_ptr<TupleSender> > predecessors;
  

 public:
   virtual void process(boost::shared_ptr<Tuple> t);
   virtual void add_pred (boost::shared_ptr<TupleSender> d) { predecessors.push_back(d); }
   virtual void clear_preds () { predecessors.clear(); }


GENERIC_CLNAME
};

}

#endif
