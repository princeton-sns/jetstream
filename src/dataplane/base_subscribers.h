#ifndef Q_SUBSCRIBER_2FAEJ0UJ
#define Q_SUBSCRIBER_2FAEJ0UJ

#include <boost/shared_ptr.hpp>

#include <boost/thread.hpp>
#include <boost/date_time/posix_time/posix_time.hpp>


#include "subscriber.h"
#include "dataplaneoperator.h"
#include "cube.h"

#include "jetstream_types.pb.h"
#include <glog/logging.h>

namespace jetstream {
namespace cube {

class QueueSubscriber: public Subscriber {
  public:
    QueueSubscriber(): Subscriber (), returnAction(SEND) {};
    virtual ~QueueSubscriber() {};

    Action returnAction;
    std::vector<boost::shared_ptr<jetstream::Tuple> > insert_q;
    std::vector<boost::shared_ptr<jetstream::Tuple> > update_q;

    virtual Action action_on_tuple(boost::shared_ptr<const jetstream::Tuple> const update);

    virtual void post_insert(boost::shared_ptr<jetstream::Tuple> const &update,
                                 boost::shared_ptr<jetstream::Tuple> const &new_value);

    virtual void post_update(boost::shared_ptr<jetstream::Tuple> const &update,
                                 boost::shared_ptr<jetstream::Tuple> const &new_value,
                                 boost::shared_ptr<jetstream::Tuple> const &old_value);

};

class UnionSubscriber: public Subscriber {
  public:
    UnionSubscriber(): Subscriber (){};
    virtual ~UnionSubscriber() {};

    virtual Action action_on_tuple(boost::shared_ptr<const jetstream::Tuple> const update);

    virtual void post_insert(boost::shared_ptr<jetstream::Tuple> const &update,
                                 boost::shared_ptr<jetstream::Tuple> const &new_value);

    virtual void post_update(boost::shared_ptr<jetstream::Tuple> const &update,
                                 boost::shared_ptr<jetstream::Tuple> const &new_value,
                                 boost::shared_ptr<jetstream::Tuple> const &old_value);

};

} /* cube */

class Querier {

 public:
    operator_err_t configure(std::map<std::string,std::string> &config, operator_id_t);
    cube::CubeIterator do_query();

    jetstream::Tuple min;
    jetstream::Tuple max;
    void set_cube(DataCube *c) {cube = c;}

 protected:
    operator_id_t id;
    std::list<std::string> sort_order;
    DataCube * cube;
    int32_t num_results; //a limit on the number of results returned. 0 = infinite

};

/***
Takes as configuration a set of dimensions, including a distinguished time dimension;
also takes a time interval and start time. Each interval, it queries for all
tuples matching those dimensions, with time since the last start point.

This subscriber does no backfill. 
*/
class TimeBasedSubscriber: public jetstream::cube::Subscriber {
  private:
    static const int DEFAULT_WINDOW_OFFSET = 100; //ms
  
    volatile bool running;
    boost::shared_ptr<boost::thread> loopThread;

    int windowSizeMs;  //query interval
    int ts_field; //which field is the timestamp?
    int32_t windowOffsetMs; //how far back from 'now' the window is defined as ending; ms
//    int32_t maxTsSeen;
  
//    boost::mutex mutex; //protects next_window_start_time
    time_t next_window_start_time; //all data from before this should be visible
    int32_t backfill_tuples;  // a counter; this will be a little sloppy because of data that arrives while a query is running.
        //estimate will tend to be high: some of this data still arrived "in time"
   Querier querier;
  public:
    TimeBasedSubscriber(): running(true),next_window_start_time(0),backfill_tuples(0) {};

    virtual ~TimeBasedSubscriber() {};

    virtual Action action_on_tuple(boost::shared_ptr<const jetstream::Tuple> const update) ;

    virtual void post_insert(boost::shared_ptr<jetstream::Tuple> const &update,
                                 boost::shared_ptr<jetstream::Tuple> const &new_value);

    virtual void post_update(boost::shared_ptr<jetstream::Tuple> const &update,
                                 boost::shared_ptr<jetstream::Tuple> const &new_value, 
                                 boost::shared_ptr<jetstream::Tuple> const &old_value);
  
    virtual operator_err_t configure(std::map<std::string,std::string> &config);
    virtual void start();
    virtual void stop() {
      LOG(INFO) << id() << " received stop()";
      running = false;
    }

    void operator()();  // A thread that will loop while reading the file

    virtual std::string long_description();


  private:
    const static std::string my_type_name;


  public:
    virtual const std::string& typename_as_str() {
      return my_type_name;
    }
};

/**
*  A subscriber for measuring latency. Input must include a double, corresponding
* to the current time in miliseconds since the epoch, and a string, corresponding to a hostname.
* These are time_tuple_index and hostname_tuple_index, respectively.
* 
*/
class LatencyMeasureSubscriber: public jetstream::cube::Subscriber {
  public:
    LatencyMeasureSubscriber(): Subscriber(){};
    virtual ~LatencyMeasureSubscriber() {};

    virtual Action action_on_tuple(boost::shared_ptr<const jetstream::Tuple> const update);

    virtual void post_insert(boost::shared_ptr<jetstream::Tuple> const &update,
                                 boost::shared_ptr<jetstream::Tuple> const &new_value);

    virtual void post_update(boost::shared_ptr<jetstream::Tuple> const &update,
                                 boost::shared_ptr<jetstream::Tuple> const &new_value,
                                 boost::shared_ptr<jetstream::Tuple> const &old_value);


    virtual operator_err_t configure(std::map<std::string,std::string> &config);

    virtual void start();
    virtual void stop() {
      LOG(INFO) << id() << " received stop()";
      if (running) {
        running = false;
        loopThread->join();
      }
    }
    void operator()();  // A thread that will loop


  protected:
    unsigned int time_tuple_index;
    unsigned int hostname_tuple_index;
    unsigned int interval_ms;
    bool cumulative;
    msec_t max_seen_tuple_before_ms; //before db insertion
    msec_t max_seen_tuple_after_ms;
    msec_t start_time_ms;   //start time of current bucket. Only relevant to skew. Currently unused.
    mutable boost::mutex lock;

    std::map<std::string, std::map<int, unsigned int> > stats_before_rt; //hostname=>bucket=>count
    std::map<std::string, std::map<int, unsigned int> > stats_before_skew; //hostname=>bucket=>count
    std::map<std::string, std::map<int, unsigned int> > stats_after_rt; //hostname=>bucket=>count
    std::map<std::string, std::map<int, unsigned int> > stats_after_skew; //hostname=>bucket=>count

    int get_bucket(int latency);

    void make_stats(msec_t tuple_time_ms,  std::map<int, unsigned int> &bucket_map_rt,
     std::map<int, unsigned int> &bucket_map_skew, msec_t& max_seen_tuple_ms);
    
    void print_stats(std::map<std::string, std::map<int, unsigned int> > & stats, const char * label);

    
    boost::shared_ptr<boost::thread> loopThread;
    volatile bool running;
};

} /* jetsream */

#endif /* end of include guard: Q_SUBSCRIBER_2FAEJ0UJ */
