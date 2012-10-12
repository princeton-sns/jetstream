#include "tuple_batch.h"

using namespace std;
using namespace jetstream::cube;

TupleBatch::TupleBatch(jetstream::DataCube * cube, size_t batch):  cube(cube), batch(batch){};
TupleBatch::~TupleBatch () {};

jetstream::DataCube* TupleBatch::get_cube() { return cube; }

size_t const TupleBatch::INVALID_POSITION =  std::numeric_limits<size_t>::max();

size_t TupleBatch::insert_tuple(boost::shared_ptr<jetstream::Tuple> t, bool batch, bool need_new_value, bool need_old_value)
{
  if(batch)
  {
    return batch_add(t, need_new_value, need_old_value);
  }
  else
  {
    save_tuple(t, need_new_value, need_old_value);
    return TupleBatch::INVALID_POSITION; 
  }

}

size_t 
TupleBatch::update_batched_tuple(size_t pos, boost::shared_ptr<jetstream::Tuple> t, bool batch)
{
  if(batch)
  {
    boost::shared_ptr<jetstream::Tuple> orig = get_stored_tuple(pos);
    DataCube *cube = get_cube();
    cube->merge_tuple_into(*orig, *t);
    batch_set(orig, need_new_value_store[pos], need_old_value_store[pos], pos);
    return pos;
  }
  else
  {
    boost::shared_ptr<jetstream::Tuple> orig = remove_tuple(pos);
    DataCube* cube = get_cube();
    cube->merge_tuple_into(*orig, *t);
    save_tuple(orig, need_new_value_store[pos], need_old_value_store[batch]);
    return  TupleBatch::INVALID_POSITION; 
  }
   
}

void TupleBatch::save_tuple(boost::shared_ptr<jetstream::Tuple> t, bool need_new_value, bool need_old_value)
{
   boost::shared_ptr<jetstream::Tuple> new_tuple;
   boost::shared_ptr<jetstream::Tuple> old_tuple;
   cube->save_tuple(*t,need_new_value, need_old_value, new_tuple, old_tuple);
}

void TupleBatch::flush()
{
  assert(holes.empty());
  
  std::list<boost::shared_ptr<jetstream::Tuple> > new_tuple_list;
  std::list<boost::shared_ptr<jetstream::Tuple> > old_tuple_list;
  
  cube->save_tuple_batch(tuple_store, need_new_value_store, need_old_value_store, new_tuple_list, old_tuple_list);

  std::map<DataCube::DimensionKey, boost::shared_ptr<jetstream::Tuple> > new_tuples; 
  std::map<DataCube::DimensionKey, boost::shared_ptr<jetstream::Tuple> > old_tuples; 


  for(std::list<boost::shared_ptr<jetstream::Tuple> >::iterator it = new_tuple_list.begin();
      it != new_tuple_list.end(); ++it)
  {
    new_tuples.insert(pair<DataCube::DimensionKey, boost::shared_ptr<jetstream::Tuple> >(cube->get_dimension_key(*(*it)), *it));
  }
  for(std::list<boost::shared_ptr<jetstream::Tuple> >::iterator it = old_tuple_list.begin();
      it != old_tuple_list.end(); ++it)
  {
    old_tuples.insert(pair<DataCube::DimensionKey, boost::shared_ptr<jetstream::Tuple> >(cube->get_dimension_key(*(*it)), *it));
  }
  for(std::vector<boost::shared_ptr<jetstream::Tuple> >::iterator it = tuple_store.begin();
      it != tuple_store.end(); ++it)
  {
    DataCube::DimensionKey key = cube->get_dimension_key(*(*it));
    cube->save_callback(key, *it, new_tuples[key], old_tuples[key]);
  }
  tuple_store.clear();
  need_new_value_store.clear();
  need_old_value_store.clear();
}


size_t TupleBatch::batch_add(boost::shared_ptr<jetstream::Tuple> t, bool need_new_value, bool need_old_value)
{
  if(!holes.empty())
  {
    int pos = holes.front();
    holes.pop_front();
    return batch_set(t, need_new_value, need_old_value, pos);
  }
  
  tuple_store.push_back(t);
  need_new_value_store.push_back(need_new_value);
  need_old_value_store.push_back(need_old_value);
  
  if(tuple_store.size() >= batch)
  {
    flush();
    return  TupleBatch::INVALID_POSITION;
  }
  return tuple_store.size()-1;
}

size_t TupleBatch::batch_set(boost::shared_ptr<jetstream::Tuple> t, bool need_new_value, bool need_old_value, size_t pos)
{
  tuple_store[pos] = t;
  need_new_value_store[pos] = need_new_value;
  need_old_value_store[pos] = need_old_value;
  return pos;
}

boost::shared_ptr<jetstream::Tuple> TupleBatch::get_stored_tuple(size_t pos)
{
  return tuple_store[pos];
}

boost::shared_ptr<jetstream::Tuple> TupleBatch::remove_tuple(size_t pos)
{
  holes.push_back(pos);
  return tuple_store[pos];
}


