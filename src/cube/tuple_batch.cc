#include "tuple_batch.h"
#include <glog/logging.h>

using namespace std;
using namespace jetstream::cube;

TupleBatch::TupleBatch(jetstream::DataCube * cube, size_t batch):  cube(cube), batch(batch){};
TupleBatch::~TupleBatch () {};

jetstream::DataCube* TupleBatch::get_cube() { return cube; }

void TupleBatch::insert_tuple(boost::shared_ptr<jetstream::TupleProcessingInfo> tpi, bool batch)
{
  if(batch)
  {
    batch_add(tpi);
  }
  else
  {
    save_tuple(tpi);
  }

}

void 
TupleBatch::update_batched_tuple(boost::shared_ptr<jetstream::TupleProcessingInfo> tpi, bool batch)
{
  size_t pos = lookup[tpi->key];
  if(batch)
  {
    batch_set(tpi, pos);
  }
  else
  {
    remove_tuple(pos);
    save_tuple(tpi);
  }
   
}

void TupleBatch::set_max_batch_size(size_t size)
{
  batch = size;
} 

void TupleBatch::save_tuple(boost::shared_ptr<jetstream::TupleProcessingInfo> tpi)
{
   boost::shared_ptr<jetstream::Tuple> new_tuple;
   boost::shared_ptr<jetstream::Tuple> old_tuple;
   cube->save_tuple(*(tpi->t), tpi->need_new_value, tpi->need_old_value, new_tuple, old_tuple);
   cube->save_callback(*tpi, new_tuple, old_tuple);
}

bool TupleBatch::contains(DimensionKey key)
{
  return (lookup.count(key) > 0);
}

boost::shared_ptr<jetstream::TupleProcessingInfo> TupleBatch::get(DimensionKey key){
  assert(lookup.count(key) > 0);
  size_t pos = lookup[key];
  return tpi_store[pos];
}

void TupleBatch::flush()
{

  for(std::list<size_t>::iterator iHole = holes.begin(); iHole != holes.end(); ++iHole) {
    tpi_store.erase(tpi_store.begin()+(*iHole));
  }

  holes.clear();

  std::vector<boost::shared_ptr<jetstream::Tuple> > tuple_store;
  std::vector<bool> need_new_value_store;
  std::vector<bool> need_old_value_store;

  for(std::vector<boost::shared_ptr<jetstream::TupleProcessingInfo> >::iterator iTpi = tpi_store.begin(); iTpi != tpi_store.end(); ++iTpi) {
    tuple_store.push_back((*iTpi)->t);
    need_new_value_store.push_back((*iTpi)->need_new_value);
    need_old_value_store.push_back((*iTpi)->need_old_value);
  }
    
  std::list<boost::shared_ptr<jetstream::Tuple> > new_tuple_list;
  std::list<boost::shared_ptr<jetstream::Tuple> > old_tuple_list;
  
  cube->save_tuple_batch(tuple_store, need_new_value_store, need_old_value_store, new_tuple_list, old_tuple_list);

  std::map<jetstream::DimensionKey, boost::shared_ptr<jetstream::Tuple> > new_tuples; 
  std::map<jetstream::DimensionKey, boost::shared_ptr<jetstream::Tuple> > old_tuples; 

  for(std::list<boost::shared_ptr<jetstream::Tuple> >::iterator it = new_tuple_list.begin();
      it != new_tuple_list.end(); ++it)
  {
    new_tuples.insert(pair<jetstream::DimensionKey, boost::shared_ptr<jetstream::Tuple> >(cube->get_dimension_key(*(*it)), *it));
  }
  for(std::list<boost::shared_ptr<jetstream::Tuple> >::iterator it = old_tuple_list.begin();
      it != old_tuple_list.end(); ++it)
  {
    old_tuples.insert(pair<jetstream::DimensionKey, boost::shared_ptr<jetstream::Tuple> >(cube->get_dimension_key(*(*it)), *it));
  }

  for(std::vector<boost::shared_ptr<jetstream::TupleProcessingInfo> >::iterator iTpi = tpi_store.begin(); iTpi != tpi_store.end(); ++iTpi) {
   cube->save_callback(*(*iTpi), new_tuples[(*iTpi)->key], old_tuples[(*iTpi)->key]);
  }
  tpi_store.clear();
  lookup.clear();
}


void TupleBatch::batch_add(boost::shared_ptr<jetstream::TupleProcessingInfo> tpi)
{
  size_t pos;
  if(!holes.empty())
  {
    pos = holes.front();
    holes.pop_front();
    batch_set(tpi, pos);
  }
  else
  {
    tpi_store.push_back(tpi);
    pos = tpi_store.size()-1;
    lookup[tpi->key] = pos; 
  }  
}

void TupleBatch::batch_set(boost::shared_ptr<jetstream::TupleProcessingInfo> tpi, size_t pos)
{
  tpi_store[pos] = tpi;
  lookup[tpi->key] = pos; 
}

boost::shared_ptr<jetstream::TupleProcessingInfo> TupleBatch::get_stored_tuple(size_t pos)
{
  return tpi_store[pos];
}

boost::shared_ptr<jetstream::TupleProcessingInfo> TupleBatch::remove_tuple(size_t pos)
{
  holes.push_back(pos);
  return tpi_store[pos];
}

bool TupleBatch::is_full()
{
  return holes.empty() && tpi_store.size() >= batch;
}

bool TupleBatch::is_empty()
{
  return (size() == 0);
}

size_t TupleBatch::size()
{
  assert(tpi_store.size() >= holes.size());
  return tpi_store.size() - holes.size();
}
