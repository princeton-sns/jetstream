#ifndef JetStream_cube_h
#define JetStream_cube_h

#include <iterator>
#include <vector>
#include <list>
#include <map>
#include <boost/functional/hash.hpp>
#include "dataplaneoperator.h"  //needed only for Receiver
#include "cube_iterator.h"
#include "subscriber.h"
#include "queue_congestion_mon.h"
#include "chained_queue_mon.h"
#include "node_config.h"

#include <boost/shared_ptr.hpp>
#include "jetstream_types.pb.h"
#include "js_executor.h"

namespace jetstream {
class DataCube;
class TupleProcessingInfo;
typedef std::string DimensionKey;
}

#include "tuple_batch.h"

namespace jetstream {

/**
*  A class to represent a cube in memory.
*/


class TupleProcessingInfo {
  public:
    TupleProcessingInfo(boost::shared_ptr<Tuple> t, DimensionKey key, boost::shared_ptr<std::vector<unsigned int> > levels): t(t), key(key), levels(levels),  need_new_value(false), need_old_value(false) {};
    boost::shared_ptr<Tuple> t;
    DimensionKey key;
    boost::shared_ptr<std::vector<unsigned int> > levels;
    std::list<operator_id_t> insert;
    std::list<operator_id_t> update;
    bool need_new_value;
    bool need_old_value;
};

class ProcessCallable {

  public:
    ProcessCallable(DataCube * cube, std::string name);
    ~ProcessCallable();

    void run();

    void assign(boost::shared_ptr<Tuple> t, DimensionKey key, boost::shared_ptr<std::vector<unsigned int> > levels);
    boost::shared_ptr<cube::TupleBatch> batch_flush();
    bool batcher_ready();

  private:
    std::string name;
    boost::thread internal_thread;
    shared_ptr<io_service> service;
    io_service::work work;
    jetstream::DataCube * cube;

    // This runs in the internal thread
    void process(boost::shared_ptr<Tuple> t, DimensionKey key, boost::shared_ptr<std::vector<unsigned int> > levels);

    boost::shared_ptr<cube::TupleBatch> tupleBatcher;
    mutable boost::mutex batcherLock; // protects tupleBatcher
};



class DataCube : public TupleReceiver {
  public:
    typedef jetstream::DimensionKey DimensionKey;

    DataCube(jetstream::CubeSchema _schema, std::string _name, const NodeConfig &conf);
    virtual ~DataCube() {}

    //main external functions
    virtual void create() = 0;
    virtual void destroy() = 0;

    /**
     * @brief insert the tuple for processing. Note that it is not safe to alter the tuple after calling process
     * (until do_process returns).
     *
     * @param t
     */

    virtual void process(boost::shared_ptr<Tuple> t);
    virtual void process(boost::shared_ptr<Tuple> t, const operator_id_t src) {
      process(t);
    }

    void wait_for_commits();


    //query functions
    static unsigned int const LEAF_LEVEL;
    virtual boost::shared_ptr<jetstream::Tuple>
    get_cell_value(jetstream::Tuple const &t, std::vector<unsigned int> const &levels, bool final = true) const= 0;

    virtual cube::CubeIterator
    slice_query(jetstream::Tuple const &min, jetstream::Tuple const& max,
                bool final = true, std::list<std::string> const &sort = std::list<std::string>(),
                size_t limit = 0) const = 0;

    virtual cube::CubeIterator rollup_slice_query(std::vector<unsigned int> const &levels,
        jetstream::Tuple const &min, jetstream::Tuple const &max, bool final = true,
        std::list<std::string> const &sort = std::list<std::string>(), size_t limit = 0) const = 0;

    virtual void
    do_rollup(std::vector<unsigned int> const &levels,jetstream::Tuple const &min, jetstream::Tuple const& max) = 0;


    // info/utility functions
    virtual jetstream::cube::CubeIterator end() const = 0;
    virtual size_t num_leaf_cells() const = 0;
    Tuple empty_tuple();
    const jetstream::CubeSchema& get_schema() ;
    virtual std::string id_as_str() ;
    virtual const std::string& typename_as_str() ;
    virtual void no_more_tuples() {};

    virtual void merge_tuple_into(jetstream::Tuple &into, jetstream::Tuple const &update) const=0;

    virtual std::vector<size_t> dimension_offset(std::string) = 0; //offsets in tuples
    virtual std::vector<size_t> aggregate_offset(std::string) = 0;

    virtual size_t num_dimensions() = 0;
    virtual size_t num_aggregates() = 0;


    /**
    * It's possible to mark a cube as locked. The intended use of this is to allow
    * graceful deletion. The deleter marks the cube as frozen. As updates to the cube fail,
    * data sources drop their pointer. When the last smart pointer is removed,
    * the cube is deleted.
    *
    * Possibly a different mechanism is needed to do visibility control.
    *
    */
    void mark_as_deleted() {
      is_frozen = true;
    }

    //subscriber stuff
    void add_subscriber(boost::shared_ptr<cube::Subscriber> sub);
    void remove_subscriber(boost::shared_ptr<cube::Subscriber> sub);
    void remove_subscriber(operator_id_t id);

    //only used by tuple batch
    virtual DimensionKey get_dimension_key(Tuple const &t, boost::shared_ptr<std::vector<unsigned int> > levels) const = 0;
    virtual void get_dimension_key(const Tuple &t, boost::shared_ptr<std::vector<unsigned int> > levels,  std::ostringstream &ostr) const = 0;
    virtual boost::shared_ptr<std::vector<unsigned int> > get_leaf_levels() const = 0;

    void save_callback(jetstream::TupleProcessingInfo &tpi,
                       boost::shared_ptr<jetstream::Tuple> new_tuple, boost::shared_ptr<jetstream::Tuple> old_tuple);

  virtual void save_tuple(jetstream::Tuple const &t, bool need_new_value, bool need_old_value,
                            boost::shared_ptr<jetstream::Tuple> &new_tuple,boost::shared_ptr<jetstream::Tuple> &old_tuple)=0;


  virtual void save_tuple_batch(const std::vector<boost::shared_ptr<jetstream::Tuple> > &tuple_store,
       const std::vector<bool> &need_new_value_store, const std::vector<bool> &need_old_value_store,
       std::vector<boost::shared_ptr<jetstream::Tuple> > &new_tuple_store, std::vector<boost::shared_ptr<jetstream::Tuple> > &old_tuple_store) = 0 ;

  virtual void save_tuple_batch(const std::vector<boost::shared_ptr<jetstream::Tuple> > &tuple_store,
       const std::vector<boost::shared_ptr<std::vector<unsigned int> > > &levels_store,
       const std::vector<bool> &need_new_value_store, const std::vector<bool> &need_old_value_store,
       std::vector<boost::shared_ptr<jetstream::Tuple> > &new_tuple_store, std::vector<boost::shared_ptr<jetstream::Tuple> > &old_tuple_store) =0;

/*

    virtual void save_tuple_batch(std::vector<boost::shared_ptr<jetstream::Tuple> > tuple_store,
                                  std::vector<bool> need_new_value_store, std::vector<bool> need_old_value_store,
                                  std::list<boost::shared_ptr<jetstream::Tuple> > &new_tuple_list,
                                  std::list<boost::shared_ptr<jetstream::Tuple> > &old_tuple_list)=0;*/


    virtual boost::shared_ptr<CongestionMonitor> congestion_monitor() { return processCongestMon;}

    virtual void meta_from_upstream(const DataplaneMessage & msg, const operator_id_t pred);

    virtual void do_process(boost::shared_ptr<Tuple> t, DimensionKey key, boost::shared_ptr<std::vector<unsigned int> > levels, boost::shared_ptr<cube::TupleBatch> &tupleBatcher);

    virtual bool is_unrolled(std::vector<unsigned int> levels) const = 0;

    boost::shared_ptr<ChainedQueueMonitor> process_congestion_monitor() { return processCongestMon;}

  protected:
    jetstream::CubeSchema schema;
    std::string name;
    bool is_frozen;


    std::map<operator_id_t, boost::shared_ptr<jetstream::cube::Subscriber> > subscribers;
    mutable boost::shared_mutex subscriberLock; // protects list of operators; reader/writer semantics

    void set_current_levels(const std::vector<unsigned int> &levels);
    void set_current_levels(boost::shared_ptr<std::vector<unsigned int> > levels);

    uint64_t version;

  private:
    static const std::string my_tyepename;


    virtual void check_flush();
    void post_flush();
    Executor flushExec;

  protected:
    size_t current_processor;
    boost::shared_ptr<QueueCongestionMonitor> flushCongestMon;
    boost::shared_ptr<ChainedQueueMonitor> processCongestMon;

    std::vector<boost::shared_ptr<ProcessCallable> > processors;
    boost::shared_ptr<std::vector<unsigned int> > current_levels;
};

}

#endif
