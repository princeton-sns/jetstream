#ifndef JetStream_latency_measure_subscriber_h
#define JetStream_latency_measure_subscriber_h

#include <boost/shared_ptr.hpp>

#include <boost/thread.hpp>
#include <boost/date_time/posix_time/posix_time.hpp>

//TODO should break this dependency
#include "base_subscribers.h"

namespace jetstream {

/**
*  A subscriber for measuring latency. Input must include a double, corresponding
* to the current time in miliseconds since the epoch, and a string, corresponding to a hostname.
* These are time_tuple_index and hostname_tuple_index, respectively.
*
*/
class LatencyMeasureSubscriber: public jetstream::ThreadedSubscriber {
  public:
    LatencyMeasureSubscriber(): ThreadedSubscriber(){};
    virtual ~LatencyMeasureSubscriber() {};

    virtual Action action_on_tuple(boost::shared_ptr<const jetstream::Tuple> const update);

    virtual void start();

    virtual void post_insert(boost::shared_ptr<jetstream::Tuple> const &update,
                                 boost::shared_ptr<jetstream::Tuple> const &new_value);

    virtual void post_update(boost::shared_ptr<jetstream::Tuple> const &update,
                                 boost::shared_ptr<jetstream::Tuple> const &new_value,
                                 boost::shared_ptr<jetstream::Tuple> const &old_value);


    virtual operator_err_t configure(std::map<std::string,std::string> &config);

    virtual void operator()();  // A thread that will loop

    virtual void process(boost::shared_ptr<Tuple> t) {
      process_c(t);
      //emit(t);
    }
    void process_c(boost::shared_ptr<const Tuple>);


  protected:
    unsigned int time_tuple_index;
    unsigned int hostname_tuple_index;
    unsigned int interval_ms;
    bool cumulative;
    msec_t max_seen_tuple_before_ms; //before db insertion
    msec_t max_seen_tuple_after_ms;
    msec_t start_time_ms;   //start time of current bucket. Only relevant to skew. Currently unused.
    mutable boost::mutex lock; //locking around the data structures we build

    std::map<std::string, std::map<int, unsigned int> > stats_before_rt; //hostname=>bucket=>count
    std::map<std::string, std::map<int, unsigned int> > stats_before_skew; //hostname=>bucket=>count
    std::map<std::string, std::map<int, unsigned int> > stats_after_rt; //hostname=>bucket=>count
    std::map<std::string, std::map<int, unsigned int> > stats_after_skew; //hostname=>bucket=>count

    int get_bucket(int latency);

    void make_stats(msec_t tuple_time_ms,  std::map<int, unsigned int> &bucket_map_rt,
     std::map<int, unsigned int> &bucket_map_skew, msec_t& max_seen_tuple_ms);

    void print_stats(std::map<std::string, std::map<int, unsigned int> > & stats, const char * label);
};

}

#endif
