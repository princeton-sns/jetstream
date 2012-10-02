#ifndef JetStream_cube_h
#define JetStream_cube_h

#include <iterator>
#include <vector>
#include <list>
#include "dataplaneoperator.h"  //needed only for Receiver
#include "cube_iterator.h"


#include "jetstream_types.pb.h"


namespace jetstream {

/**
*  A class to represent a cube in memory. 
*/

class DataCube : public TupleReceiver {
  
public:
  static unsigned int const LEAF_LEVEL;

  virtual void process(boost::shared_ptr<Tuple> t) { insert_entry(*t); }  //inserts a tuple
  virtual void no_more_tuples() {};

  
  DataCube(jetstream::CubeSchema _schema, std::string _name) : 
    schema(_schema), name(_name) {};
    
  virtual ~DataCube() {}

  virtual bool insert_entry(jetstream::Tuple const &t) = 0;
  virtual bool insert_partial_aggregate(jetstream::Tuple const&t) = 0;
  
  virtual boost::shared_ptr<jetstream::Tuple> get_cell_value(jetstream::Tuple const &t, bool final = true) const= 0;

  virtual cube::CubeIterator slice_query(jetstream::Tuple const &min, jetstream::Tuple const& max, bool final = true, std::list<std::string> const &sort = std::list<std::string>(), size_t limit = 0) const = 0;
  
 // virtual cube::CubeIterator rollup(jetstream::Tuple const &min, jetstream::Tuple const& max, std::list<unsigned int> const &levels, bool final = true) = 0;
  
  virtual jetstream::cube::CubeIterator end() const = 0;

  virtual size_t num_leaf_cells() const = 0;
  
  virtual void create() = 0;
  virtual void destroy() = 0;
  
  Tuple empty_tuple() {
    Tuple t;
    for (int i=0; i < schema.dimensions_size(); ++i) {
      t.add_e();
    }
    return t;
  }
  
  const jetstream::CubeSchema& get_schema() { return schema; }
  virtual std::string id_as_str() { return name; }
  virtual const std::string& typename_as_str() { return my_tyepename; }


  /**
  * It's possible to mark a cube as locked. The intended use of this is to allow
  * graceful deletion. The deleter marks the cube as frozen. As updates to the cube fail,
  * data sources drop their pointer. When the last smart pointer is removed,
  * the cube is deleted. 
  *
  * Possibly a different mechanism is needed to do visibility control.  
  *
  */
  void mark_as_deleted() {is_frozen = true; }

  //iterator<forward_iterator_tag, Tuple> stream_tuples(Tuple k);
  

//TODO: should have an entry here for the aggregation/update function.
  
protected:
  jetstream::CubeSchema schema;
  std::string name;
  bool is_frozen;
//TODO should figure out how to implement this

private:
  static const std::string my_tyepename;
  
};

}

#endif
