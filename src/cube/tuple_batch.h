#ifndef TUPLE_BATCH_NYYMCL31
#define TUPLE_BATCH_NYYMCL31

#include <vector>
#include <list>

namespace jetstream { namespace cube {
class TupleBatch; 
}}

#include "cube.h"
  
namespace jetstream {
namespace cube {

class TupleBatch {

public:
  TupleBatch(jetstream::DataCube * cube, size_t batch);
  virtual ~TupleBatch ();

  void 
    insert_tuple(boost::shared_ptr<jetstream::TupleProcessingInfo> tpi, bool batch);
  void 
    update_batched_tuple(boost::shared_ptr<jetstream::TupleProcessingInfo> tpi, bool batch);

  void flush();
  bool is_full();
  bool is_empty();
  size_t size();
  bool contains(jetstream::DimensionKey key);
  boost::shared_ptr<jetstream::TupleProcessingInfo> get(DimensionKey key);
private:

  void save_tuple(boost::shared_ptr<jetstream::TupleProcessingInfo> tpi);
  void batch_add(boost::shared_ptr<jetstream::TupleProcessingInfo> tpi);
  void batch_set(boost::shared_ptr<jetstream::TupleProcessingInfo> tpi, size_t pos);

  boost::shared_ptr<jetstream::TupleProcessingInfo> get_stored_tuple(size_t pos);
  boost::shared_ptr<jetstream::TupleProcessingInfo> remove_tuple(size_t pos);


  jetstream::DataCube * get_cube();
  jetstream::DataCube * cube;
  size_t batch;
  std::vector<boost::shared_ptr<jetstream::TupleProcessingInfo> > tpi_store;  
  std::map<DimensionKey, size_t> lookup;
  std::list<size_t> holes;
};


  
} /* cube */
} /* jetstream */



#endif /* end of include guard: TUPLE_BATCH_NYYMCL31 */
