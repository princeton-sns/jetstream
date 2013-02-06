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
#define HISTOGRAM_SIZE 40

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

struct histo_storage {
   jetstream::LogHistogram *hist_ptr;
   char * buffer;
   size_t max_buffer_size;
};

/*
 * Output the library version.
 * merge_histogram()
 */

my_bool merge_histogram_init(UDF_INIT *initid, UDF_ARGS *args, char *message) {
  if (args->arg_count != 1) {
    strcpy(message,"merge_histogram requires one argumente");
    return 1;
  }
  if (args->arg_type[0] != STRING_RESULT) {
    strcpy(message,"merge_histogram requires a string parameter");
    return 1;
  }

  initid->const_item = 0;
  initid->maybe_null = 0;
  initid->max_length = 64*1024; //64KB ... can also be 16MB
  //initid->ptr is a char * used to communicate allocated memory
  //

  histo_storage *store= new histo_storage;
  store->hist_ptr = NULL;
  store->buffer = (char *)malloc(sizeof(char)*1024);
  store->max_buffer_size = 1024;
  initid->ptr = (char *) store;
  return 0; //no error occured

  //if error:
  //x_strlcpy(message, "No arguments allowed (udf: lib_mysqludf_str_info)", MYSQL_ERRMSG_SIZE);
  //return 1;
}


void merge_histogram_reset(UDF_INIT *initid, UDF_ARGS *args, char *is_null, char *error) {
  merge_histogram_clear(initid, is_null, error);
  merge_histogram_add(initid, args, is_null, error);
}

void merge_histogram_clear(UDF_INIT *initid, char *is_null, char *error) {
  histo_storage *store= (histo_storage *)initid->ptr;

  if(store->hist_ptr != NULL) {
    delete store->hist_ptr;
  }

  store->hist_ptr = new jetstream::LogHistogram(HISTOGRAM_SIZE);
}

void merge_histogram_add(UDF_INIT *initid, UDF_ARGS *args, char *is_null, char *error) {
  //each argument can be one column of a row here we just want one column

  //database stores protobuf
  char *serProtoBuf = args->args[0];
  unsigned long serSz = args->lengths[0];

  //get histogram to add
  jetstream::JSSummary summary;
  summary.ParseFromArray(serProtoBuf, serSz);
  const jetstream::JSHistogram& add_hist = summary.histo();

  //add in histogream
  histo_storage *store= (histo_storage *)initid->ptr;
  (store->hist_ptr)->merge_in(add_hist);
}

void merge_histogram_deinit(UDF_INIT *initid) {
  histo_storage *store= (histo_storage *)initid->ptr;
  if(store->hist_ptr != NULL) {
    delete store->hist_ptr;
  }
  free(store->buffer);
  delete store;
}

char* merge_histogram(UDF_INIT *initid, UDF_ARGS *args, char* result, unsigned long* length,	char *is_null, char *error) {
  histo_storage *store= (histo_storage *)initid->ptr;
  jetstream::LogHistogram *hist_ptr = store->hist_ptr;

  jetstream::JSSummary summary;
  hist_ptr->serialize_to(summary);

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
