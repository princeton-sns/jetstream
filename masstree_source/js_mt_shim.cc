
#include "js_mt_shim.h"
//#include "libmasstree.hh"
#include "masstree.hh"
#include "masstree_query.hh"
#include "masstree_struct.hh"
#include "masstree_key.hh"
#include "masstree_traverse.hh"
#include "masstree_scan.hh"
#include "string.hh"

#include "kvrow.hh"

using std::cout;
using std::endl;
typedef lcdf::Str str;


/* Scanner used to scan to get the next key from Masstree. */
namespace {
struct scan_next {
    char key_[1024];
    int keylen_;
    int i_;
    scan_next()
	: keylen_(0), i_(0) {
    }
    bool operator()(const str &key, row_type *, threadinfo *) {
        memcpy(key_, key.s, key.len);
	keylen_ = key.len;
	return false;
    }
};

/* Scanner used to scan to get the next key from Masstree. */
struct scan_last {
    char key_[1024];
    int keylen_;
    scan_last()
	: keylen_(0) {
    }
    bool operator()(const str &key, row_type *, threadinfo *) {
	memcpy(key_, key.s, key.len);
	//printf("SCANNED last: %s, len: %d\n", key.s, key.len);
	keylen_ = key.len;
	return true;
    }
};

/* Scanner used for seeking to a particular key, denoted by goal_. */
struct scan_movetonext {
    int keylen_;
    char goal_[1024];
    int goallen_;
    int ret_;
    int unpackedFlag_;
    int unpackedPrefix_;
    char key_[1024];
    scan_movetonext(char *goal, int goallen, int uF, int uP)
	: keylen_(0), goallen_(goallen), ret_(-1), unpackedFlag_(uF), unpackedPrefix_(uP)  {
        memcpy(goal_, goal, goallen);
    }
 
    bool operator()(const str &key, row_type *, threadinfo *) {
	int ret = memcmp(key.data(), goal_, goallen_);
        // This is a consequence of Btree orderings, which can be found in the
        // sqlite3VdbeRecordCompare function.
        if (ret == 0) {
          if (unpackedFlag_) {
            ret = -1;
          } else if (unpackedPrefix_) {

    	  } else if (key.len > goallen_) {
      	    ret = 1;
          }
        }
  	uint64_t tester;
 	memcpy(&tester, key.s+1, sizeof(uint64_t));
        if (ret < 0) {
	    memcpy(key_, key.s, key.len);
	    keylen_ = key.len;
	    ret_ = -1;
	    return true;
	}
	else if (ret > 0) {
	    memcpy(key_, key.s, key.len);
	    keylen_ = key.len;
	    ret_ = 1;
	    return false;
        }
        else {
	    memcpy(key_, key.s, key.len);
	    keylen_ = key.len;
	    ret_ = 0;
	    return false;
        }
    }
};
}

inline threadinfo * get_ti() {
  threadinfo * ti = threadinfo::current();
  
  if (!ti) {
    cout << "Making a new thread_info" << endl;
    ti = threadinfo::make(threadinfo::TI_MAIN, -1);
  } 

  if (!ti) {
    cout << "ERR: no thread_info" << endl;
    exit(1);
  }
  ti->enter();

  return ti;
}


JSMasstree::JSMasstree() {
    masstree_table = new mt_table;
    
//    threadinfo * ti = get_ti();
    threadinfo * ti = threadinfo::make(threadinfo::TI_MAIN, -1);
    ti->enter();
    masstree_table->initialize(ti);
}


JSMasstree::~JSMasstree() {
  delete masstree_table;
}

int
JSMasstree::elements() const {
  threadinfo * ti = get_ti();

  scan_last scanner;
  return masstree_table->table().scan( str( (char * )0, 0), true, scanner, ti);
}


void
JSMasstree::clear() {
  delete masstree_table;
  masstree_table = new mt_table;

  threadinfo * ti = get_ti();
  masstree_table->initialize(ti);
}


JSMScanner JSMasstree::scan(const std::string& start, const std::string& end) const {
  JSMScanner s(start, end);
  return s;
}

void
JSMasstree::set(const char * key, const char * data, size_t data_size) {
  query<row_type> q;

  str data_buf = str(data,data_size);
  threadinfo * ti = get_ti();
/*
	kvout * kvo_ = new_kvout(-1, 2048);
  Str req = row_type::make_put_col_request(kvo_, valueindex_string(0, data_size), data_buf);
  cout << "Request len is " << req.length() << std::endl;
  q.begin_put(key,req);
  */
  q.begin_replace(key,data_buf);
//  cout << "setting to '" << data_buf << "' (@" << (void*)data  << "). Len is " << data_buf.len <<endl;
  masstree_table->replace(q, ti);
  
//  masstree_table->print(stdout, 0);

}


void
JSMasstree::get(const char * key, char * data, size_t& data_size) const {

  query<row_type> q;
  threadinfo * ti = get_ti();
  bool result;
  
  
  q.begin_get1(key);
//	kvout * kvo_ = new_kvout(-1, 2048);
//  q.begin_get(lcdf::Str key, lcdf::Str req, struct kvout *kvout)
  result = masstree_table->get(q, ti);
  str val = uninitialized_type();
  val = q.get1_value();
//  cout << "result is " << result << " and len is " << val.length() << endl;
//  cout << "Val:" << val<< endl;
  
  if (result && val.len <= data_size) {
    data_size = val.len;
    memcpy(data,val.s, val.len);
  }
  else
    data_size = 0;
}



void JSMasstree::set_ptr(const char * key, void * data) {
  query<row_type> q;
//  cout << "setting " << key << " to " << data << endl;
  str data_buf = str( (char*) &data, sizeof(data));
//  cout <<  "Buf holds " << *( (void **)data_buf.s) << endl;
    //At this point, *(data_buf.s) == data, and we're going to copy that into the tree
  threadinfo * ti = get_ti();
  q.begin_replace(key,data_buf);
  masstree_table->replace(q, ti);
  
//  if (DEBUG)
//    masstree_table->print(stdout, 0);
}


void * JSMasstree::get_ptr(const char * key) const {
  query<row_type> q;
  threadinfo * ti = get_ti();
  bool result;
    
  q.begin_get1(key);
  result = masstree_table->get(q, ti);
  str val = uninitialized_type();  
  val = q.get1_value();
  if (val.length() == 0)
    return 0; //key not found
  
//  cout << "read " << key << ", returned val.s is " <<  (void*)(val.s) << "(" << val.len << ")"
 //     << " pointing to " <<  *((void **)val.s)<< endl;
  return   * ((void**) val.s);
}

void
JSMScanner::next(std::string& k, char * bytesOut, size_t buf_size) {


}

bool JSMScanner::hasNext() const {
  return false;
}


kvtimestamp_t do_static_init() {
  pthread_key_create(&threadinfo::key, 0);
  return timestamp();
}
//Defs for log
volatile bool recovering = false;
volatile uint64_t globalepoch = 1;
kvtimestamp_t initial_timestamp = do_static_init();
kvepoch_t global_log_epoch = 0; //defined in log.cc
