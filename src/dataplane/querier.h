//
//  querier.h
//  JetStream
//
//  Created by Ariel Rabkin on 3/12/13.
//  Copyright (c) 2013 Ariel Rabkin. All rights reserved.
//

#ifndef JetStream_querier_h
#define JetStream_querier_h

#include "cube.h"

namespace  jetstream {


class Querier {

 public:
    operator_err_t configure(std::map<std::string,std::string> &config, operator_id_t);
    cube::CubeIterator do_query();

    jetstream::Tuple min;
    jetstream::Tuple max;
    std::vector<unsigned int> rollup_levels; //length will be zero if no rollups
    void set_cube(DataCube *c) {cube = c;}

    void tuple_inserted(const Tuple& t) {rollup_is_dirty = true;}
    void set_rollup_level(int fieldID, unsigned r_level);
    void set_rollup_levels(DataplaneMessage& m);

 protected:
    volatile bool rollup_is_dirty; //should have real rollup manager eventually.
    operator_id_t id;
    std::list<std::string> sort_order;
    DataCube * cube;
    int32_t num_results; //a limit on the number of results returned. 0 = infinite

};


}


#endif
