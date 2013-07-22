
#include "chain_ops.h"
#include <map>
#include <iostream>
#include <boost/thread/thread.hpp>
#include <boost/numeric/conversion/cast.hpp>
#include <gtest/gtest.h>

#include "experiment_operators.h"
//#include "
#include "mt_cube.h"

using namespace jetstream;
using jetstream::cube::MasstreeCube;
using boost::shared_ptr;
using std::vector;

boost::shared_ptr<jetstream::CubeSchema> get_example_schema()
{
  shared_ptr<jetstream::CubeSchema> sc(new jetstream::CubeSchema());

  jetstream::CubeSchema_Dimension * dim = sc->add_dimensions();
  dim->set_name("time");
  dim->set_type(CubeSchema_Dimension_DimensionType_TIME_HIERARCHY);
  dim->add_tuple_indexes(0);

  dim = sc->add_dimensions();
  dim->set_name("url");
  dim->set_type(CubeSchema_Dimension_DimensionType_STRING);
  dim->add_tuple_indexes(1);

  jetstream::CubeSchema_Aggregate * agg = sc->add_aggregates();
  agg->set_name("count");
  agg->set_type("count");
  agg->add_tuple_indexes(2);

  agg = sc->add_aggregates();
  agg->set_name("total_size");
  agg->set_type("count");
  agg->add_tuple_indexes(3);
  return sc;
}

TEST(MTCube, CreateAndStore) {
  shared_ptr<CubeSchema> sc = get_example_schema();
  MasstreeCube cube(*sc, "web_requests", true);
  
  jetstream::Tuple t;
  extend_tuple_time(t, time(NULL));
  extend_tuple(t, "http://www.example.com");
  extend_tuple(t, 4);
  extend_tuple(t, 40);


  boost::shared_ptr<jetstream::Tuple> new_tuple;
  boost::shared_ptr<jetstream::Tuple> old_tuple;
  cube.save_tuple(t, true, false, new_tuple, old_tuple);
  ASSERT_FALSE(old_tuple);
  ASSERT_TRUE(new_tuple.get());
  ASSERT_EQ(t.e_size(), new_tuple->e_size());
  
  ASSERT_EQ(1, cube.num_leaf_cells() );
  
    //only the dimensions in t should be checked, so we will alter values of aggs.
  t.mutable_e(2)->set_i_val(0);
  boost::shared_ptr< std::vector<unsigned> > levels = cube.get_leaf_levels();
  
    //check that we got back the stored value, not the query tuple
  new_tuple = cube.get_cell_value(t, *levels);
  ASSERT_FALSE( !new_tuple.get()); //got a cell
  
  ASSERT_EQ(4, new_tuple->e_size());
  ASSERT_EQ(40, new_tuple->e(3).i_val());
//  cube->destroy();
//  cube->create();
}



TEST(MTCube, PerfTest) {
  const int NUM_DISTINCT = 16;
  const int NUM_TUPLES = 2 * 1000 * 1000;
  const int CUBE_THREADS = 4;
  bool PROFILE_RAW = false;
  
  NodeConfig conf;
  conf.cube_processor_threads = CUBE_THREADS;
  shared_ptr<CubeSchema> sc = get_example_schema();
//  MasstreeCube* cube = new MasstreeCube(*sc, "web_requests", true);
  MasstreeCube cube(*sc, "web_requests", true, conf);

  
  vector< shared_ptr<Tuple> > tuples;
  time_t cur_time = time(NULL);
  for (int i = 0; i < NUM_DISTINCT; ++i) {
    shared_ptr<Tuple> t(new jetstream::Tuple);
    extend_tuple_time(*t, cur_time);
    std::string  url("http://www.example.com/");
    url += char('0' + i);
    extend_tuple(*t, url);
    extend_tuple(*t, 2);
    extend_tuple(*t, 20);
    tuples.push_back(t);
  }
  
  usec_t time_taken, start = get_usec();
  shared_ptr<Tuple> no_need;

  if (PROFILE_RAW) {
    for (int i = 0; i < NUM_TUPLES; ++i) {
        cube.save_tuple(*tuples[i % NUM_DISTINCT], false, false, no_need, no_need);
    }
    time_taken = get_usec() - start;
    LOG(INFO) << "Measured " << double(NUM_TUPLES) * 1000000 /  time_taken << " tuples per second (raw)";
    start = get_usec();
  }
  
  for (int i = 0; i < NUM_TUPLES; ++i) {
      cube.process(NULL, tuples[i % NUM_DISTINCT]);
  }
  time_taken = get_usec() - start;
  LOG(INFO) << "Measured " << double(NUM_TUPLES) * 1000000 /  time_taken << " tuples per second (via process())";
//  delete cube;
  
}
