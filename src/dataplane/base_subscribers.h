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

    void tuple_inserted(const Tuple& t) {rollup_is_dirty = true;}
    void set_rollup_level(int fieldID, unsigned r_level);
    void set_rollup_levels(DataplaneMessage& m);

 protected:
    volatile bool rollup_is_dirty; //should have real rollup manager eventually.
    operator_id_t id;
    std::list<std::string> sort_order;
    std::vector<unsigned int> rollup_levels; //length will be zero if no rollups
    DataCube * cube;
    int32_t num_results; //a limit on the number of results returned. 0 = infinite

};


class ThreadedSubscriber: public jetstream::cube::Subscriber {
  public:
    ThreadedSubscriber():running(false) {}

    virtual void start();
    virtual void operator()() = 0;  // A thread that will loop while reading the file
    virtual void stop() {
      LOG(INFO) << id() << " received stop(); running is " << running;
      if (running) {
        running = false;
        loopThread->interrupt();
        loopThread->join();
      }
      VLOG(1) << id() <<  " joined with loop thread";
    }


  protected:
    volatile bool running;
    boost::shared_ptr<boost::thread> loopThread;
    Querier querier;

};

/***
Takes as configuration a set of dimensions, including a distinguished time dimension;
also takes a time interval and start time. Each interval, it queries for all
tuples matching those dimensions, with time since the last start point.

This subscriber does no backfill.
*/
class TimeBasedSubscriber: public jetstream::ThreadedSubscriber {
  private:
    static const int DEFAULT_WINDOW_OFFSET = 100; //ms

    time_t start_ts;

  protected:
    int ts_field; //which field is the timestamp? in the cube
    int ts_input_tuple_index; //which field is the timestamp on the input tuple.
    int latency_ts_field; //field which has a timestamp to compare against to measure latency of propagation
//    int32_t maxTsSeen;

//    boost::mutex mutex; //protects next_window_start_time
    int32_t backfill_tuples;  // a counter; this will be a little sloppy because of data that arrives while a query is running.
    time_t last_backfill_time;
    int32_t regular_tuples;
        //estimate will tend to be high: some of this data still arrived "in time"
    int windowSizeMs;  //query interval
    int32_t windowOffsetMs; //how far back from 'now' the window is defined as ending; ms
    time_t next_window_start_time; //all data from before this should be visible
    boost::shared_ptr<CongestionPolicy> congest_policy;

    virtual void respond_to_congestion();

    void send_rollup_levels();

  public:
    TimeBasedSubscriber(): backfill_tuples(0), regular_tuples(0), next_window_start_time(0) {};

    virtual ~TimeBasedSubscriber() {};

    virtual Action action_on_tuple(boost::shared_ptr<const jetstream::Tuple> const update) ;

    virtual void post_insert(boost::shared_ptr<jetstream::Tuple> const &update,
                                 boost::shared_ptr<jetstream::Tuple> const &new_value);

    virtual void post_update(boost::shared_ptr<jetstream::Tuple> const &update,
                                 boost::shared_ptr<jetstream::Tuple> const &new_value,
                                 boost::shared_ptr<jetstream::Tuple> const &old_value);

    virtual operator_err_t configure(std::map<std::string,std::string> &config);

    void operator()();  // A thread that will loop while reading the file

    virtual std::string long_description();

    virtual void set_congestion_policy(boost::shared_ptr<CongestionPolicy> p) {
      congest_policy = p;
    }

    int window_size() {return windowSizeMs;}

  private:

    bool simulation;
    int simulation_rate;

GENERIC_CLNAME
};

/**
   Switches to rollups in the presence of congestion.
   For now, only does time rollups.
*/
class VariableCoarseningSubscriber: public jetstream::TimeBasedSubscriber {

  public:
    virtual void respond_to_congestion();
    virtual operator_err_t configure(std::map<std::string,std::string> &config);

  protected:
    int cur_level;
    std::vector<double> rollup_data_ratios;
    std::vector<unsigned> rollup_time_periods;
//    int dim_to_coarsen;

GENERIC_CLNAME
};

class OneShotSubscriber : public jetstream::ThreadedSubscriber {
  public:
    virtual Action action_on_tuple(boost::shared_ptr<const jetstream::Tuple> const update) {
      return  NO_SEND;
    }

    virtual void post_insert(boost::shared_ptr<jetstream::Tuple> const &update,
                                 boost::shared_ptr<jetstream::Tuple> const &new_value) {}

    virtual void post_update(boost::shared_ptr<jetstream::Tuple> const &update,
                                 boost::shared_ptr<jetstream::Tuple> const &new_value,
                                 boost::shared_ptr<jetstream::Tuple> const &old_value) {}

    virtual operator_err_t configure(std::map<std::string,std::string> &config);

    virtual void operator()();  // A thread that will loop while reading the file


 protected:
};

} /* jetsream */

#endif /* end of include guard: Q_SUBSCRIBER_2FAEJ0UJ */
