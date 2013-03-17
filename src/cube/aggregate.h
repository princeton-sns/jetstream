#ifndef CUBE_AGGREGATE_H
#define CUBE_AGGREGATE_H

#include <boost/shared_ptr.hpp>

#include "jetstream_types.pb.h"

namespace jetstream {
namespace cube {

using namespace ::std;
using namespace boost;

class Aggregate {
  template<typename A,typename B> friend class DataCubeImpl;
  public:
    Aggregate ();
    void init(jetstream::CubeSchema_Aggregate schema_dimension);
    virtual ~Aggregate () {};
    string get_name() const;
    virtual void update_from_delta(jetstream::Tuple & newV, const jetstream::Tuple& oldV) const = 0;

  protected:
    virtual size_t number_tuple_elements() const;
    string name;
    string type;
    vector<size_t> tuple_indexes;
};

} /* cube */
} /* jetstream */
#endif /* end of include guard: CUBE_AGGREGATE_H */
