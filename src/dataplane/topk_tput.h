#ifndef JetStream_topk_tput_h
#define JetStream_topk_tput_h

#include <glog/logging.h>

#include "jetstream_types.pb.h"
#include "dataplaneoperator.h"
#include "cube.h"
#include "subscriber.h"


namespace jetstream {

enum ProtoState {
  NOT_STARTED,
  ROUND_1,
  ROUND_2,
  ROUND_3
};

double get_rank_val(boost::shared_ptr<const jetstream::Tuple>, size_t col);


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
  void end_of_round(int round_no);
  std::list<std::string> sort_order;
  bool take_greatest;

  GENERIC_CLNAME
};



class MultiRoundCoordinator: public DataPlaneOperator {

 private:
   std::vector<boost::shared_ptr<TupleSender> > predecessors;
   unsigned int num_results;
   Tuple dim_filter_sta;
   Tuple dim_filter_end;
  
   time_t start_ts;
   int ts_field;
   int window_offset;
   std::string sort_column;
   unsigned int responses_this_phase;
   unsigned int total_col;
   boost::shared_ptr<DataCube> destcube;
   double tao_1;
   double calculate_tao();

//   std::string downstream_cube_name;
  
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

   virtual void start();

 
   virtual void process(boost::shared_ptr<Tuple> t, const operator_id_t pred);
  
   virtual void add_pred (boost::shared_ptr<TupleSender> d) { predecessors.push_back(d); }
   virtual void clear_preds () { predecessors.clear(); }
  
   virtual void meta_from_upstream(const DataplaneMessage & msg, const operator_id_t pred);

   ProtoState phase; //visible for debugging

 protected:
   void start_phase_2();
   void start_phase_3();

GENERIC_CLNAME
};

}

#endif
