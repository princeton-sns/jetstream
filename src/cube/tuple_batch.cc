#include "tuple_batch.h"

using namespace std;
using namespace jetstream::cube;


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
TupleBatch::update_batched_tuple(size_t pos, boost::shared_ptr<jetstream::Tuple> t, bool batch, bool need_new_value, bool need_old_value)
{
  if(batch)
  {
    boost::shared_ptr<jetstream::Tuple> orig = get_stored_tuple(pos);
    boost::shared_ptr<DataCube> cube = get_cube();
    cube->merge_tuple_into(*orig, *t);
    batch_set(orig, need_new_value, need_old_value, pos);
    return pos;
  }
  else
  {
    boost::shared_ptr<jetstream::Tuple> orig = remove_tuple(pos);
    boost::shared_ptr<DataCube> cube = get_cube();
    cube->merge_tuple_into(*orig, *t);
    save_tuple(orig, need_new_value, need_old_value);
    return  TupleBatch::INVALID_POSITION; 
  }
   
}

void TupleBatch::save_tuple(boost::shared_ptr<jetstream::Tuple> t, bool need_new_value, bool need_old_value)
{
   boost::shared_ptr<jetstream::Tuple> new_tuple;
   boost::shared_ptr<jetstream::Tuple> old_tuple;
  //cube->save_tuple(t,need_new_value, need_old_value, new_tuple, old_tuple);
}

void TupleBatch::flush()
{
  assert(holes.empty());
  
  std::list<boost::shared_ptr<jetstream::Tuple> > new_tuple_list;
  std::list<boost::shared_ptr<jetstream::Tuple> > old_tuple_list;
  
  //cube->save_tuple_batch(tuple_store, need_new_value_store, need_old_value_store, new_tuple_list, old_tuple_list);
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


