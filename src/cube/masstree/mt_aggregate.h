//
//  mt_aggregate.h
//  JetStream
//
//  Created by Ariel Rabkin on 7/19/13.
//  Copyright (c) 2013 Ariel Rabkin. All rights reserved.
//

#ifndef JetStream_mt_aggregate_h
#define JetStream_mt_aggregate_h

#include "js_utils.h"
#include "aggregate.h"

namespace jetstream {

class MasstreeAggregate: public ::jetstream::cube::Aggregate {

  public:
/*
    MasstreeAggregate(size_t s): sz(s) {}
    virtual size_t bytes_len() {
      return sz;
    }*/

          //clears the aggregate,where this aggregate is at agg_pos
    virtual size_t size(const Tuple& src) const = 0;
    virtual size_t from_tuple(char * agg_pos, const Tuple& src) = 0;
    virtual size_t merge(char * agg_pos, const char * src) = 0;
    virtual size_t merge(char * agg_pos, const Tuple& src) = 0;
    virtual size_t add_to_tuple(char * agg_pos, Tuple * dest ) = 0;

    virtual void merge_tuple_into(jetstream::Tuple & newV, const jetstream::Tuple& oldV) const = 0;


  protected:
//    size_t sz;
//    size_t srctuple_offset;
//    size_t aggs_offset;
};

typedef int64_t mt_sum_t;
class MTSumAggregate: public MasstreeAggregate {

  public:

    virtual size_t size(const Tuple& src) const {
       return sizeof(mt_sum_t);
    }
  
    virtual size_t from_tuple(char * agg_pos, const Tuple& src) {
      long l = src.e(tuple_indexes[0]).i_val();
      *(reinterpret_cast<mt_sum_t*>(agg_pos)) = l;
      return sizeof(mt_sum_t);
    }
  
    virtual size_t merge(char * agg_pos, const char * src) {
      *(reinterpret_cast<mt_sum_t*>(agg_pos)) +=  *(reinterpret_cast<const mt_sum_t*>(src));
      return sizeof(mt_sum_t);
    }
  
    virtual size_t merge(char * agg_pos, const Tuple& src) {
      long l = src.e(tuple_indexes[0]).i_val();
      *(reinterpret_cast<mt_sum_t*>(agg_pos)) += l;
      return sizeof(mt_sum_t);
    }

    virtual size_t add_to_tuple(char * agg_pos, Tuple * dest ) {
      long l =  *(reinterpret_cast<mt_sum_t*>(agg_pos));
      extend_tuple(*dest, (int) l);
      return sizeof(mt_sum_t);
    }

    virtual void update_from_delta(jetstream::Tuple & newV, const jetstream::Tuple& oldV) const;
    virtual void merge_tuple_into(jetstream::Tuple & newV, const jetstream::Tuple& oldV) const;
  
};


class MTVersionAggregate: public MasstreeAggregate {

  private:
    uint64_t& v;
  public:
    MTVersionAggregate(uint64_t& cube_vers): v(cube_vers) {
    }
  
    virtual size_t size(const Tuple& src) const {
      return sizeof(uint64_t);
    }

    virtual size_t from_tuple(char * agg_pos, const Tuple& src) {
      v += 1;
      //TODO
      return sizeof(uint64_t);
    }

    virtual size_t merge(char * agg_pos, const char * src) {
      //TODO
      return sizeof(uint64_t);
    }

    virtual size_t merge(char * agg_pos, const Tuple& src) {
      //TODO
      return sizeof(uint64_t);
    }
  
    virtual size_t add_to_tuple(char * agg_pos, Tuple * dest ) {
      //TODO
      return sizeof(uint64_t);
    }

    virtual void update_from_delta(jetstream::Tuple & newV, const jetstream::Tuple& oldV) const;
    virtual void merge_tuple_into(jetstream::Tuple & newV, const jetstream::Tuple& oldV) const;
};

}

#endif
