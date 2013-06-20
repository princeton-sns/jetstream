#ifndef Q_SUBSCRIBER_2FAEJ0UJ
#define Q_SUBSCRIBER_2FAEJ0UJ

#include <boost/shared_ptr.hpp>

#include <boost/thread.hpp>
#include <boost/date_time/posix_time/posix_time.hpp>


#include "subscriber.h"
#include "cube.h"

#include "jetstream_types.pb.h"
#include <glog/logging.h>
#include "querier.h"
#include "timeteller.h"


namespace jetstream {
namespace cube {

class QueueSubscriber: public Subscriber {
  public:
    QueueSubscriber(): Subscriber (), returnAction(SEND), need_new(true), need_old(true) {};
    virtual ~QueueSubscriber() {};

    Action returnAction;
    std::vector<boost::shared_ptr<jetstream::Tuple> > insert_q;
    std::vector<boost::shared_ptr<jetstream::Tuple> > update_q;

    bool need_new;
    bool need_old;

    virtual Action action_on_tuple(OperatorChain * c, boost::shared_ptr<const jetstream::Tuple> const update);
  
    virtual bool need_new_value(boost::shared_ptr<const jetstream::Tuple> const update) {
      return need_new;
    }
    virtual bool need_old_value(boost::shared_ptr<const jetstream::Tuple> const update) {
      return need_old;
    }


    virtual void post_insert(boost::shared_ptr<jetstream::Tuple> const &update,
                                 boost::shared_ptr<jetstream::Tuple> const &new_value);

    virtual void post_update(boost::shared_ptr<jetstream::Tuple> const &update,
                                 boost::shared_ptr<jetstream::Tuple> const &new_value,
                                 boost::shared_ptr<jetstream::Tuple> const &old_value);

    virtual operator_err_t configure(std::map<std::string,std::string> &config) {
      return NO_ERR;
    }
  
    virtual shared_ptr<FlushInfo> incoming_meta(const OperatorChain&,
                                                const DataplaneMessage&) {
      shared_ptr<FlushInfo> p;
      return p;
    }
    
};


} /* cube */


class StrandedSubscriber: public jetstream::cube::Subscriber {
  public:
    StrandedSubscriber():running(false) {}
    virtual ~StrandedSubscriber();

    virtual void start();
  
    virtual void chain_stopping(OperatorChain *) {
      LOG(INFO) << id() << " received stop(); running is " << running;
      if (running) {
        running = false;
      }
      if (timer)
        timer->cancel();
      chain.reset(); //to avoid leaving a cycle
    }
    void emit_wrapper();
    virtual int emit_batch() = 0;

    void stop_from_subscriber(); //NOT virtual

  protected:
    volatile bool running;
    boost::shared_ptr<boost::asio::strand> st;
    boost::shared_ptr<boost::asio::deadline_timer> timer;
  
//    boost::shared_ptr<boost::thread> loopThread;
    Querier querier;

};

/***
Takes as configuration a set of dimensions, including a distinguished time dimension;
also takes a time interval and start time. Each interval, it queries for all
tuples matching those dimensions, with time since the last start point.

This subscriber does no backfill.
*/
class TimeBasedSubscriber: public jetstream::StrandedSubscriber {
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



    int backfill_old_window; 
    int regular_old_window; 

    boost::shared_ptr<TimeTeller> tt;
    std::map<const OperatorChain*, time_t> times;
    mutable boost::mutex stateLock; //locks the times map.
        // The lock is only needed when state accessed from the upcalls from the cube.
        // Everything else is on one strand.

    virtual void respond_to_congestion();

    void send_rollup_levels();

  public:
    TimeBasedSubscriber(): ts_field(-1),ts_input_tuple_index(-1),latency_ts_field(-1),
      backfill_tuples(0), regular_tuples(0), next_window_start_time(0),
      backfill_old_window(0),regular_old_window(0) {};

    virtual ~TimeBasedSubscriber() {};
  
    virtual void start();

    unsigned int get_window_offset_sec();

    virtual Action action_on_tuple(OperatorChain * c, boost::shared_ptr<const jetstream::Tuple> const update) ;

    virtual void post_insert(boost::shared_ptr<jetstream::Tuple> const &update,
                                 boost::shared_ptr<jetstream::Tuple> const &new_value);

    virtual void post_update(boost::shared_ptr<jetstream::Tuple> const &update,
                                 boost::shared_ptr<jetstream::Tuple> const &new_value,
                                 boost::shared_ptr<jetstream::Tuple> const &old_value);

    virtual void post_flush(unsigned id);


    virtual operator_err_t configure(std::map<std::string,std::string> &config);

    virtual int emit_batch();

    virtual std::string long_description() const;

    virtual void set_congestion_policy(boost::shared_ptr<CongestionPolicy> p) {
      congest_policy = p;
    }

    int window_size() {return windowSizeMs;}
  
    virtual shared_ptr<FlushInfo> incoming_meta(const OperatorChain&,
                                                const DataplaneMessage&);
    

  private:
  
    void update_backfill_stats(int elems);

//    int simulation_rate;

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

class OneShotSubscriber : public jetstream::StrandedSubscriber {
  public:
    virtual Action action_on_tuple(OperatorChain * c, boost::shared_ptr<const jetstream::Tuple> const update) {
      return  NO_SEND;
    }

    virtual void post_insert(boost::shared_ptr<jetstream::Tuple> const &update,
                                 boost::shared_ptr<jetstream::Tuple> const &new_value) {}

    virtual void post_update(boost::shared_ptr<jetstream::Tuple> const &update,
                                 boost::shared_ptr<jetstream::Tuple> const &new_value,
                                 boost::shared_ptr<jetstream::Tuple> const &old_value) {}

    virtual operator_err_t configure(std::map<std::string,std::string> &config);

    virtual int emit_batch();
  
    virtual shared_ptr<FlushInfo> incoming_meta(const OperatorChain&,
                                                const DataplaneMessage&) {
      shared_ptr<FlushInfo> p;
      return p;
    }  

GENERIC_CLNAME
};


class DelayedOneShotSubscriber : public jetstream::OneShotSubscriber {
  public:
  
    virtual shared_ptr<FlushInfo> incoming_meta(const OperatorChain&,
                                                const DataplaneMessage&);

    virtual Action action_on_tuple(OperatorChain * c, boost::shared_ptr<const jetstream::Tuple> const update);

    virtual operator_err_t configure(std::map<std::string,std::string> &config);
    virtual int emit_batch();

    virtual void post_flush(unsigned id);


  protected:
    std::map<const OperatorChain*, msec_t> times;  //records the time OF UPDATE, locally
    std::map<const OperatorChain*, bool> former_chains; 

    mutable boost::mutex stateLock; //locks the times map.
        // The lock is only needed when state accessed from the upcalls from the cube.
        // Everything else is on one strand.
  

GENERIC_CLNAME
};

} /* jetstream */

#endif /* end of include guard: Q_SUBSCRIBER_2FAEJ0UJ */
