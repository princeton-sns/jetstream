#include "tuple_batch.h"
#include <glog/logging.h>

using namespace std;
using namespace jetstream::cube;

TupleBatch::TupleBatch(jetstream::DataCube * cube):  cube(cube) {};
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

  msec_t start_time= get_msec();
  int num_flushes = 0;
  //std::vector<boost::shared_ptr<jetstream::TupleProcessingInfo> >::iterator iTpi = tpi_store.begin();
  
  //using DimensionKey order to avoid deadlocks in the db. (DimensionKey) follows same order as PK in the DB.
  std::map<DimensionKey, size_t>::iterator iPos = lookup.begin();
  unsigned int max_elems_per_batch = std::numeric_limits<int>::max(); //was usefull before now isn't left it in to make code flexible

  while(iPos != lookup.end() ){
    unsigned int num_elems = 0;
    num_flushes++;
    std::map<DimensionKey, size_t>::iterator iStartBatchPos = iPos;
    std::vector<boost::shared_ptr<jetstream::Tuple> > tuple_store;
    std::vector<boost::shared_ptr<std::vector<unsigned int> > > levels_store;
    std::vector<bool> need_new_value_store;
    std::vector<bool> need_old_value_store;

    while(iPos != lookup.end() && num_elems < max_elems_per_batch ) {
      boost::shared_ptr<jetstream::TupleProcessingInfo> tpi = tpi_store[(*iPos).second];
      tuple_store.push_back(tpi->t);
      levels_store.push_back((tpi)->levels);
      need_new_value_store.push_back((tpi)->need_new_value);
      need_old_value_store.push_back((tpi)->need_old_value);
      ++iPos;
      ++num_elems;
    }

    boost::shared_ptr<jetstream::Tuple> empty_ptr;
    std::vector<boost::shared_ptr<jetstream::Tuple> > new_tuple_store(tuple_store.size(), empty_ptr);
    std::vector<boost::shared_ptr<jetstream::Tuple> > old_tuple_store(tuple_store.size(), empty_ptr);

    cube->save_tuple_batch(tuple_store, levels_store, need_new_value_store, need_old_value_store, new_tuple_store, old_tuple_store);

    int i = 0;
    for(;iStartBatchPos != iPos; ++iStartBatchPos)
    {
      boost::shared_ptr<jetstream::TupleProcessingInfo> tpi = tpi_store[(*iStartBatchPos).second];
      cube->save_callback(*tpi, new_tuple_store[i], old_tuple_store[i]);
      i++;
    }
  }
  msec_t now = get_msec();
  LOG_FIRST_N(INFO, 10) << "Finished flush in "<< now-start_time <<" of "
    << tpi_store.size() <<" elements with "
    << num_flushes << " flushes. Rate = "<<  (now-start_time >0 ? tpi_store.size()/(now-start_time):0);
  /*
  std::map<jetstream::DimensionKey, boost::shared_ptr<jetstream::Tuple> > new_tuples;
  std::map<jetstream::DimensionKey, boost::shared_ptr<jetstream::Tuple> > old_tuples;

  //TODO: can we remove the get_dimension_key calls


  std::ostringstream tmpostr;
  for(std::list<boost::shared_ptr<jetstream::Tuple> >::iterator it = new_tuple_list.begin();
      it != new_tuple_list.end(); ++it)
  {
    tmpostr.str("");
    tmpostr.clear();
    cube->get_dimension_key(*(*it), tmpostr);
    DimensionKey key = tmpostr.str();
    new_tuples.insert(pair<jetstream::DimensionKey, boost::shared_ptr<jetstream::Tuple> >(key, *it));
  }
  for(std::list<boost::shared_ptr<jetstream::Tuple> >::iterator it = old_tuple_list.begin();
      it != old_tuple_list.end(); ++it)
  {
    tmpostr.str("");
    tmpostr.clear();
    cube->get_dimension_key(*(*it), tmpostr);
    DimensionKey key = tmpostr.str();
    old_tuples.insert(pair<jetstream::DimensionKey, boost::shared_ptr<jetstream::Tuple> >(key, *it));
  }

  for(std::vector<boost::shared_ptr<jetstream::TupleProcessingInfo> >::iterator iTpi = tpi_store.begin(); iTpi != tpi_store.end(); ++iTpi) {
   cube->save_callback(*(*iTpi), new_tuples[(*iTpi)->key], old_tuples[(*iTpi)->key]);
  }*/
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

bool TupleBatch::is_empty()
{
  return (size() == 0);
}

size_t TupleBatch::size()
{
  assert(tpi_store.size() >= holes.size());
  return tpi_store.size() - holes.size();
}

void TupleBatch::clear()
{
  holes.clear();
  tpi_store.clear();
  lookup.clear();
}
