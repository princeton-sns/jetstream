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
#define HISTOGRAM_SIZE 30
#define RESERVOIR_SAMPLE_SIZE 30

#include "jetstream_types.pb.h"
#include "quantile_est.h"


/* For Windows, define PACKAGE_STRING in the VS project */
//#ifndef __WIN__
//#include "config.h"
//#endif



/* These must be right or mysqld will not find the symbol! */
#ifdef	__cplusplus
extern "C" {
#endif
  DLLEXP my_bool merge_histogram_init(UDF_INIT *initid, UDF_ARGS *args, char *message);
  DLLEXP void merge_histogram_deinit(UDF_INIT *initid);
  DLLEXP char *merge_histogram(UDF_INIT *initid, UDF_ARGS *args, char *result, unsigned long *length, char *is_null, char *error);

  DLLEXP void merge_histogram_reset(UDF_INIT *initid, UDF_ARGS *args, char *is_null, char *error);
  DLLEXP void merge_histogram_clear(UDF_INIT *initid, char *is_null, char *error);
  DLLEXP void merge_histogram_add(UDF_INIT *initid, UDF_ARGS *args, char *is_null, char *error);

#ifdef	__cplusplus
}
#endif

template<class Aggregate>
Aggregate *create_new()
{
  assert(0);
}

template<class Aggregate>
struct storage {
   Aggregate *aggregate_ptr;
   char * buffer;
   size_t max_buffer_size;
};

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
  initid->maybe_null = 0;
  initid->max_length = 64*1024; //64KB ... can also be 16MB
  //initid->ptr is a char * used to communicate allocated memory
  //

  storage<Aggregate> *store= new storage<Aggregate>();
  store->aggregate_ptr = NULL;
  store->buffer = (char *)malloc(sizeof(char)*1024);
  store->max_buffer_size = 1024;

  initid->ptr = (char *) store;
  return 0; //no error occured

  //if error:
  //x_strlcpy(message, "No arguments allowed (udf: lib_mysqludf_str_info)", MYSQL_ERRMSG_SIZE);
  //return 1;
}

template<class Aggregate>
void merge_reset(UDF_INIT *initid, UDF_ARGS *args, char *is_null, char *error) {
  merge_histogram_clear(initid, is_null, error);
  merge_histogram_add(initid, args, is_null, error);
}

template<class Aggregate>
void merge_clear(UDF_INIT *initid, char *is_null, char *error) {
  storage<Aggregate> *store= (storage<Aggregate> *)initid->ptr;

  if(store->aggregate_ptr != NULL) {
    delete store->aggregate_ptr;
  }

  store->aggregate_ptr = create_new<Aggregate>();
}

template<class Aggregate>
void merge_add(UDF_INIT *initid, UDF_ARGS *args, char *is_null, char *error) {
  //each argument can be one column of a row here we just want one column

  //database stores protobuf
  char *serProtoBuf = args->args[0];
  unsigned long serSz = args->lengths[0];

  //get histogram to add
  jetstream::JSSummary summary;
  summary.ParseFromArray(serProtoBuf, serSz);

  Aggregate update_agg(summary);
  storage<Aggregate> *store= (storage<Aggregate> *)initid->ptr;
  Aggregate * agg_ptr = store->aggregate_ptr;
  agg_ptr->merge_in(update_agg);
}

template<class Aggregate>
void merge_deinit(UDF_INIT *initid) {
  storage<Aggregate> *store= (storage<Aggregate> *)initid->ptr;
  if(store->aggregate_ptr != NULL) {
    delete store->aggregate_ptr;
  }
  free(store->buffer);
  delete store;
}

template<class Aggregate>
char* merge(UDF_INIT *initid, UDF_ARGS *args, char* result, unsigned long* length,	char *is_null, char *error) {
  storage<Aggregate> *store= (storage<Aggregate> *)initid->ptr;
  Aggregate * agg_ptr = store->aggregate_ptr;

  jetstream::JSSummary summary;
  agg_ptr->serialize_to(summary);

  size_t msg_sz = summary.ByteSize();
  if(store->max_buffer_size < msg_sz) {
    size_t min_sz = msg_sz < store->max_buffer_size * 2? store->max_buffer_size*2: msg_sz; //max(msg_sz, store->max_buffer_size * 2)
    char *buffer = (char *) realloc(store->buffer, min_sz*sizeof(char));
    if(buffer == NULL) {
      *error = 1;
      return NULL;
    }
    store->buffer = buffer;
    store->max_buffer_size = min_sz;
  }

  summary.SerializeToArray(store->buffer, msg_sz);
  *length = msg_sz;
  *is_null = 0;
  *error = 0;
  return result;
}



//////////////////////// Histograms /////////////////////////////////////


template<>
jetstream::LogHistogram *create_new<jetstream::LogHistogram>(){
  return new jetstream::LogHistogram(HISTOGRAM_SIZE);
}

my_bool merge_histogram_init(UDF_INIT *initid, UDF_ARGS *args, char *message) {
  return merge_init<jetstream::LogHistogram>(initid, args, message);
}
void merge_histogram_reset(UDF_INIT *initid, UDF_ARGS *args, char *is_null, char *error) {
  merge_reset<jetstream::LogHistogram>(initid, args, is_null, error);
}
void merge_histogram_clear(UDF_INIT *initid, char *is_null, char *error) {
  merge_clear<jetstream::LogHistogram>(initid, is_null, error);
}
void merge_histogram_add(UDF_INIT *initid, UDF_ARGS *args, char *is_null, char *error) {
  merge_add<jetstream::LogHistogram>(initid, args, is_null, error);
}
void merge_histogram_deinit(UDF_INIT *initid) {
  merge_deinit<jetstream::LogHistogram>(initid);
}

char* merge_histogram(UDF_INIT *initid, UDF_ARGS *args, char* result, unsigned long* length,char *is_null, char *error) {
      return merge<jetstream::LogHistogram>(initid, args, result, length, is_null, error) ;
}


/////////////////////// samples ///////////////////////

template<>
jetstream::ReservoirSample *create_new<jetstream::ReservoirSample>(){
  return new jetstream::ReservoirSample(RESERVOIR_SAMPLE_SIZE);
}

my_bool merge_reservoir_sample_init(UDF_INIT *initid, UDF_ARGS *args, char *message) {
  return merge_init<jetstream::ReservoirSample>(initid, args, message);
}
void merge_reservoir_sample_reset(UDF_INIT *initid, UDF_ARGS *args, char *is_null, char *error) {
  merge_reset<jetstream::ReservoirSample>(initid, args, is_null, error);
}
void merge_reservoir_sample_clear(UDF_INIT *initid, char *is_null, char *error) {
  merge_clear<jetstream::ReservoirSample>(initid, is_null, error);
}
void merge_reservoir_sample_add(UDF_INIT *initid, UDF_ARGS *args, char *is_null, char *error) {
  merge_add<jetstream::ReservoirSample>(initid, args, is_null, error);
}
void merge_reservoir_sample_deinit(UDF_INIT *initid) {
  merge_deinit<jetstream::ReservoirSample>(initid);
}

char* merge_reservoir_sample(UDF_INIT *initid, UDF_ARGS *args, char* result, unsigned long* length,char *is_null, char *error) {
      return merge<jetstream::ReservoirSample>(initid, args, result, length, is_null, error) ;
}


