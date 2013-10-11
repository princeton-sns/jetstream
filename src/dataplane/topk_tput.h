#ifndef JetStream_topk_tput_h
#define JetStream_topk_tput_h

#include <glog/logging.h>

#include "jetstream_types.pb.h"
#include "chain_ops.h"
#include "cube.h"
#include "subscriber.h"


namespace jetstream {

enum ProtoState {
  NOT_STARTED,
  ROUND_1,
  ROUND_2,
  ROUND_3,
  DONE
};

double get_rank_val(boost::shared_ptr<const jetstream::Tuple>, size_t col);


class MultiRoundSender: public jetstream::cube::Subscriber {

public:
    MultiRoundSender(): Subscriber () {};
    virtual ~MultiRoundSender() {};

    virtual Action action_on_tuple(OperatorChain * c, boost::shared_ptr<const jetstream::Tuple> const update);

    virtual void post_insert(boost::shared_ptr<jetstream::Tuple> const &update,
                                 boost::shared_ptr<jetstream::Tuple> const &new_value);

    virtual void post_update(boost::shared_ptr<jetstream::Tuple> const &update,
                                 boost::shared_ptr<jetstream::Tuple> const &new_value,
                                 boost::shared_ptr<jetstream::Tuple> const &old_value);

    virtual shared_ptr<FlushInfo> incoming_meta(const OperatorChain&,
                                                const DataplaneMessage&) {
      shared_ptr<FlushInfo> p;
      return p;
    }


    virtual void meta_from_downstream(DataplaneMessage & msg);

    void get_bounds(Tuple & my_min, Tuple & my_max, const Tuple & q, int time_col);
  
    operator_err_t configure(std::map<std::string,std::string> &config) {
      return NO_ERR;
    }
  
    virtual void start();

private:
  void end_of_round(int round_no);
  std::list<std::string> sort_order;
  bool take_greatest;
  Tuple min, max;
  std::vector<unsigned> rollup_levels;
  boost::shared_ptr<boost::asio::strand> st;  

  GENERIC_CLNAME
};


typedef boost::recursive_mutex tput_mutex;
class MultiRoundCoordinator: public TimerSource {

 private:
   std::vector<boost::shared_ptr<OperatorChain> > predecessors;
   std::map<OperatorChain *, boost::shared_ptr<OperatorChain> > future_preds;

   unsigned int num_results;
   unsigned int min_window_size; //seconds
   Tuple dim_filter_start;
   Tuple dim_filter_end;
   std::vector<unsigned> rollup_levels;
  
   time_t start_ts;
   int ts_field;
   int window_offset;
   std::string sort_column;
   bool just_once;
  
   unsigned int responses_this_phase;
   unsigned int total_col;
   boost::shared_ptr<DataCube> destcube;
   double tau_1;
   long long bw_start;
  
   double calculate_tau();

//   std::string downstream_cube_name;
  
  tput_mutex mutex;
//  boost::shared_ptr<boost::asio::deadline_timer> timer;
  
  struct CandidateItem {
    double val;
    size_t responses;
    Tuple example;
    
    CandidateItem():val(0), responses(0) {}
    
    CandidateItem(double v, size_t r, const Tuple & t):val(v), responses(r) {
      example.CopyFrom(t);
    }
  };
  
   std::map<DimensionKey, CandidateItem> candidates;

 public:
 
   virtual operator_err_t configure(std::map<std::string,std::string> &config);

   virtual bool is_source() {return true;} //so we get a chain of our own, even without incoming connections
   virtual void start();
//   virtual void stop();

   virtual void add_chain(boost::shared_ptr<OperatorChain>);
   virtual void chain_stopping(OperatorChain * c);
  
   virtual int emit_data();
  
   virtual void process( OperatorChain * c,
                         std::vector<boost::shared_ptr<Tuple> >& t,
                         DataplaneMessage & msg);
  
//   virtual void meta_from_upstream(const DataplaneMessage & msg, const operator_id_t pred);

   ProtoState phase; //visible for debugging

 protected:
   void start_phase_1(time_t window_end);
   void start_phase_2();
   void start_phase_3();
//   void wait_for_restart();

GENERIC_CLNAME
};

}

#endif
