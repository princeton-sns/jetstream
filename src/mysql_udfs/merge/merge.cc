/*
 * Skeleton libary for MySQL.
 * A set of MySQL user defined functions (UDF) to [DESCRIPTION]
 *
 * Copyright (C) [YYYY YOUR NAME <YOU@EXAMPLE.COM>]
 *
 * This library is free software; you can redistribute it and/or modify it
 * under the terms of the GNU Lesser General Public License as published by
 * the Free Software Foundation; either version 2.1 of the License, or (at
 * your option) any later version.
 *
 * This library is distributed in the hope that it will be useful, but
 * WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU Lesser
 * General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public License
 * along with this library; if not, write to the Free Software Foundation, Inc.,
 * 59 Temple Place, Suite 330, Boston, MA 02111-1307 USA
 */

#define STANDARD 1
#include "mysqludf.h"

#include "jetstream_types.pb.h"
#include "quantile_est.h"
#include "cm_sketch.h"
#include <iostream>

//set max lengths to 64KB for now, can also be 16mb
#define MAX_LENGTH_PAIR  64*1024
#define MAX_LENGTH_AGG  64*1024




/* For Windows, define PACKAGE_STRING in the VS project */
//#ifndef __WIN__
//#include "config.h"
//#endif



/* These must be right or mysqld will not find the symbol! */
/*#ifdef	__cplusplus*/
//extern "C" {
//#endif
//DLLEXP my_bool merge_histogram_init(UDF_INIT *initid, UDF_ARGS *args, char *message);
//DLLEXP void merge_histogram_deinit(UDF_INIT *initid);
//DLLEXP char *merge_histogram(UDF_INIT *initid, UDF_ARGS *args, char *result, unsigned long *length, char *is_null, char *error);

//DLLEXP void merge_histogram_reset(UDF_INIT *initid, UDF_ARGS *args, char *is_null, char *error);
//DLLEXP void merge_histogram_clear(UDF_INIT *initid, char *is_null, char *error);
//DLLEXP void merge_histogram_add(UDF_INIT *initid, UDF_ARGS *args, char *is_null, char *error);

//#ifdef	__cplusplus
//}
/*#endif*/

struct storage {
  jetstream::JSSummary *summary_ptr;
  char * buffer;
  size_t max_buffer_size;
};

void get_summary_from_param(UDF_ARGS *args, int param_index, jetstream::JSSummary &summary) {
  //database stores protobuf
  char *serProtoBuf = args->args[param_index];
  unsigned long serSz = args->lengths[param_index];

  summary.ParseFromArray(serProtoBuf, serSz);
}

size_t fill_buffer(const jetstream::JSSummary &summary, storage *store) {
  size_t msg_sz = summary.ByteSize();

  if(store->max_buffer_size < msg_sz) {
    size_t min_sz = msg_sz < store->max_buffer_size * 2? store->max_buffer_size*2: msg_sz; //max(msg_sz, store->max_buffer_size * 2)
    char *buffer = (char *) realloc(store->buffer, min_sz*sizeof(char));

    if(buffer == NULL) {
      return 0;
    }

    store->buffer = buffer;
    store->max_buffer_size = min_sz;
  }

  summary.SerializeToArray(store->buffer, msg_sz);
  return msg_sz;
}

template<class Aggregate>
my_bool merge_init(UDF_INIT *initid, UDF_ARGS *args, char *message) {
  if (args->arg_count != 1) {
    strcpy(message,"merge requires one argumente");
    return 1;
  }

  if (args->arg_type[0] != STRING_RESULT) {
    strcpy(message,"merge requires a string parameter(the blob)");
    return 1;
  }

  initid->const_item = 0;
  initid->maybe_null = 1;
  initid->max_length = MAX_LENGTH_AGG;
  //initid->ptr is a char * used to communicate allocated memory
  //

  storage *store= new storage();
  store->summary_ptr = NULL;
  store->buffer = (char *)malloc(sizeof(char)*1024);
  store->max_buffer_size = 1024;

  initid->ptr = (char *) store;
  return 0; //no error occured
}

template<class Aggregate>
void merge_clear(UDF_INIT *initid, char *is_null, char *error) {
  storage *store= (storage *)initid->ptr;

  if(store->summary_ptr != NULL) {
    delete store->summary_ptr;
    store->summary_ptr = NULL;
  }
}

template<class Aggregate>
void merge_add(UDF_INIT *initid, UDF_ARGS *args, char *is_null, char *error) {
  //each argument can be one column of a row here we just want one column

  //get histogram to add
  jetstream::JSSummary summary;
  get_summary_from_param(args, 0, summary);

  storage *store= (storage *)initid->ptr;

  if(NULL == store->summary_ptr) {
    store->summary_ptr = new jetstream::JSSummary(summary);
    return;
  }
  else {
   jetstream::JSSummary *into = store->summary_ptr;
   jetstream::merge_summary<Aggregate>(*into, summary);
  }
}

template<class Aggregate>
void merge_reset(UDF_INIT *initid, UDF_ARGS *args, char *is_null, char *error) {
  merge_clear<Aggregate>(initid, is_null, error);
  merge_add<Aggregate>(initid, args, is_null, error);
}

template<class Aggregate>
void merge_deinit(UDF_INIT *initid) {
  storage *store= (storage *)initid->ptr;

  if(store->summary_ptr != NULL) {
    delete store->summary_ptr;
    store->summary_ptr = NULL;
  }

  free(store->buffer);
  delete store;
}

template<class Aggregate>
char* merge(UDF_INIT *initid, UDF_ARGS *args, char* result, unsigned long* length,	char *is_null, char *error) {
  storage *store= (storage *)initid->ptr;
  jetstream::JSSummary * sum_ptr = store->summary_ptr;

  if (NULL == sum_ptr) {
    *length = 0;
    *is_null = 1;
    return NULL;
  }

  size_t msg_sz = fill_buffer(*sum_ptr, store);

  if(0 == msg_sz) {
    *error = 1;
    *is_null = 0;
    *length = 0;
    return NULL;
  }

  *length = msg_sz;
  *is_null = 0;
  *error = 0;
  return store->buffer;
}

template<class Aggregate>
my_bool merge_pair_init(UDF_INIT *initid, UDF_ARGS *args, char *message) {
  if (args->arg_count != 2) {
    strcpy(message,"merge_pair requires two argumente");
    return 1;
  }

  if (args->arg_type[0] != STRING_RESULT && args->arg_type[1] != STRING_RESULT) {
    strcpy(message,"merge_pair requires two string parameter(the blob)");
    return 1;
  }

  initid->const_item = 0;
  initid->maybe_null = 1;
  initid->max_length = MAX_LENGTH_PAIR; //64KB ... can also be 16MB
  //initid->ptr is a char * used to communicate allocated memory
  //

  storage *store= new storage();
  store->summary_ptr = NULL;
  store->buffer = (char *)malloc(sizeof(char)*1024);
  store->max_buffer_size = 1024;

  initid->ptr = (char *) store;
  return 0; //no error occured
}

template<class Aggregate>
void merge_pair_deinit(UDF_INIT *initid) {
  storage *store= (storage *)initid->ptr;

  assert(store->summary_ptr == NULL); //LOG_IF(FATAL, store->summary_ptr != NULL) << "summary ptr should be null";

  free(store->buffer);
  delete store;
}


template<class Aggregate>
char* merge_pair(UDF_INIT *initid, UDF_ARGS *args, char* result, unsigned long* length,	char *is_null, char *error) {
  storage *store= (storage *)initid->ptr;

  jetstream::JSSummary sum_lhs;
  get_summary_from_param(args, 0, sum_lhs);

  jetstream::JSSummary sum_rhs;
  get_summary_from_param(args, 1, sum_rhs);

  jetstream::merge_summary<Aggregate>(sum_lhs, sum_rhs);

  size_t msg_sz = fill_buffer(sum_lhs, store);

  if(0 == msg_sz) {
    *error = 1;
    *is_null = 0;
    *length = 0;
    return NULL;
  }

  *length = msg_sz;
  *is_null = 0;
  *error = 0;
  return store->buffer;
}





#ifdef	__cplusplus
extern "C" {
#endif

//////////////////////// Histograms /////////////////////////////////////

  DLLEXP my_bool merge_histogram_init(UDF_INIT *initid, UDF_ARGS *args, char *message) {
    return merge_init<jetstream::LogHistogram>(initid, args, message);
  }
  DLLEXP void merge_histogram_reset(UDF_INIT *initid, UDF_ARGS *args, char *is_null, char *error) {
    merge_reset<jetstream::LogHistogram>(initid, args, is_null, error);
  }
  DLLEXP void merge_histogram_clear(UDF_INIT *initid, char *is_null, char *error) {
    merge_clear<jetstream::LogHistogram>(initid, is_null, error);
  }
  DLLEXP void merge_histogram_add(UDF_INIT *initid, UDF_ARGS *args, char *is_null, char *error) {
    merge_add<jetstream::LogHistogram>(initid, args, is_null, error);
  }
  DLLEXP void merge_histogram_deinit(UDF_INIT *initid) {
    merge_deinit<jetstream::LogHistogram>(initid);
  }
  DLLEXP char* merge_histogram(UDF_INIT *initid, UDF_ARGS *args, char* result, unsigned long* length,char *is_null, char *error) {
    return merge<jetstream::LogHistogram>(initid, args, result, length, is_null, error) ;
  }


  DLLEXP my_bool merge_pair_histogram_init(UDF_INIT *initid, UDF_ARGS *args, char *message) {
    return merge_pair_init<jetstream::LogHistogram>(initid, args, message);
  }
  DLLEXP void merge_pair_histogram_deinit(UDF_INIT *initid) {
    merge_pair_deinit<jetstream::LogHistogram>(initid);
  }
  DLLEXP char* merge_pair_histogram(UDF_INIT *initid, UDF_ARGS *args, char* result, unsigned long* length,char *is_null, char *error) {
    return merge_pair<jetstream::LogHistogram>(initid, args, result, length, is_null, error) ;
  }

/////////////////////// samples ///////////////////////

  DLLEXP my_bool merge_reservoir_sample_init(UDF_INIT *initid, UDF_ARGS *args, char *message) {
    return merge_init<jetstream::ReservoirSample>(initid, args, message);
  }
  DLLEXP void merge_reservoir_sample_reset(UDF_INIT *initid, UDF_ARGS *args, char *is_null, char *error) {
    merge_reset<jetstream::ReservoirSample>(initid, args, is_null, error);
  }
  DLLEXP void merge_reservoir_sample_clear(UDF_INIT *initid, char *is_null, char *error) {
    merge_clear<jetstream::ReservoirSample>(initid, is_null, error);
  }
  DLLEXP void merge_reservoir_sample_add(UDF_INIT *initid, UDF_ARGS *args, char *is_null, char *error) {
    merge_add<jetstream::ReservoirSample>(initid, args, is_null, error);
  }
  DLLEXP void merge_reservoir_sample_deinit(UDF_INIT *initid) {
    merge_deinit<jetstream::ReservoirSample>(initid);
  }
  DLLEXP char* merge_reservoir_sample(UDF_INIT *initid, UDF_ARGS *args, char* result, unsigned long* length,char *is_null, char *error) {
    //this is the way to log
    //std::cerr << "in merge reservoir sample v 1"<<std::endl;
    return merge<jetstream::ReservoirSample>(initid, args, result, length, is_null, error) ;
  }

  DLLEXP my_bool merge_pair_reservoir_sample_init(UDF_INIT *initid, UDF_ARGS *args, char *message) {
    return merge_pair_init<jetstream::ReservoirSample>(initid, args, message);
  }
  DLLEXP void merge_pair_reservoir_sample_deinit(UDF_INIT *initid) {
    merge_pair_deinit<jetstream::ReservoirSample>(initid);
  }
  DLLEXP char* merge_pair_reservoir_sample(UDF_INIT *initid, UDF_ARGS *args, char* result, unsigned long* length,char *is_null, char *error) {
    return merge_pair<jetstream::ReservoirSample>(initid, args, result, length, is_null, error) ;
  }

//////////////////////// Sketches  /////////////////////////////////////

  DLLEXP my_bool merge_sketch_init(UDF_INIT *initid, UDF_ARGS *args, char *message) {
    return merge_init<jetstream::CMMultiSketch>(initid, args, message);
  }
  DLLEXP void merge_sketch_reset(UDF_INIT *initid, UDF_ARGS *args, char *is_null, char *error) {
    merge_reset<jetstream::CMMultiSketch>(initid, args, is_null, error);
  }
  DLLEXP void merge_sketch_clear(UDF_INIT *initid, char *is_null, char *error) {
    merge_clear<jetstream::CMMultiSketch>(initid, is_null, error);
  }
  DLLEXP void merge_sketch_add(UDF_INIT *initid, UDF_ARGS *args, char *is_null, char *error) {
    merge_add<jetstream::CMMultiSketch>(initid, args, is_null, error);
  }
  DLLEXP void merge_sketch_deinit(UDF_INIT *initid) {
    merge_deinit<jetstream::CMMultiSketch>(initid);
  }
  DLLEXP char* merge_sketch(UDF_INIT *initid, UDF_ARGS *args, char* result, unsigned long* length,char *is_null, char *error) {
    return merge<jetstream::CMMultiSketch>(initid, args, result, length, is_null, error) ;
  }


  DLLEXP my_bool merge_pair_sketch_init(UDF_INIT *initid, UDF_ARGS *args, char *message) {
    return merge_pair_init<jetstream::CMMultiSketch>(initid, args, message);
  }
  DLLEXP void merge_pair_sketch_deinit(UDF_INIT *initid) {
    merge_pair_deinit<jetstream::CMMultiSketch>(initid);
  }
  DLLEXP char* merge_pair_sketch(UDF_INIT *initid, UDF_ARGS *args, char* result, unsigned long* length,char *is_null, char *error) {
    return merge_pair<jetstream::CMMultiSketch>(initid, args, result, length, is_null, error) ;
  }





#ifdef	__cplusplus
}   //closes extern "C"
#endif
