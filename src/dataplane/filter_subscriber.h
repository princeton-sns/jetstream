
#ifndef JetStream_filter_subscriber_h
#define JetStream_filter_subscriber_h

#include "subscriber.h"


namespace  jetstream {

class FilterSubscriber: public cube::Subscriber {
  public:
    FilterSubscriber(): Subscriber (), filter_bound(0),level_in_field(0),cube_field(-1) {};
    virtual ~FilterSubscriber() {};

    virtual Action action_on_tuple(boost::shared_ptr<const jetstream::Tuple> const update);

    virtual void post_insert(boost::shared_ptr<jetstream::Tuple> const &update,
                                 boost::shared_ptr<jetstream::Tuple> const &new_value);

    virtual void post_update(boost::shared_ptr<jetstream::Tuple> const &update,
                                 boost::shared_ptr<jetstream::Tuple> const &new_value,
                                 boost::shared_ptr<jetstream::Tuple> const &old_value);

    virtual operator_err_t configure(std::map<std::string,std::string> &config);

    virtual void process(OperatorChain * chain, std::vector<boost::shared_ptr<Tuple> > &, DataplaneMessage&);

  

  protected:
    int filter_bound;
    unsigned level_in_field; //field of tuple inputs to set filter
    int cube_field; //field id of tuples from cube

};

}


#endif
