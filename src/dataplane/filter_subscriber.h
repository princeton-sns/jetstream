
#ifndef JetStream_filter_subscriber_h
#define JetStream_filter_subscriber_h

#include "subscriber.h"


namespace  jetstream {

class FilterSubscriber: public cube::Subscriber {
  public:
    FilterSubscriber(): Subscriber (){};
    virtual ~FilterSubscriber() {};

    virtual Action action_on_tuple(boost::shared_ptr<const jetstream::Tuple> const update);

    virtual void post_insert(boost::shared_ptr<jetstream::Tuple> const &update,
                                 boost::shared_ptr<jetstream::Tuple> const &new_value);

    virtual void post_update(boost::shared_ptr<jetstream::Tuple> const &update,
                                 boost::shared_ptr<jetstream::Tuple> const &new_value,
                                 boost::shared_ptr<jetstream::Tuple> const &old_value);

    virtual operator_err_t configure(std::map<std::string,std::string> &config);

    virtual void process(boost::shared_ptr<Tuple> t);


};

}


#endif
