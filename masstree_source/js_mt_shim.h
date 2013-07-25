/*
A shim to make MassTree friendly for Jetstream

*/
#include <stdlib.h>
#include <utility>
#include <string>

#ifndef JS_MT_SHIM
#define JS_MT_SHIM

namespace Masstree {

template <typename R> class query_table;
template <typename R> class basic_table;
struct default_query_table_params;
}
//typedef Masstree::basic_table<Masstree::default_query_table_params> mt_table;
typedef Masstree::query_table<Masstree::default_query_table_params> mt_table;

class value_array;
typedef value_array row_type;

class JSMasstree;
class threadinfo;
namespace lcdf{
struct Str;
}

class JSMScanner {
  public:
    JSMScanner(const std::string& s, const std::string & e):
      start(s), end(e), cur_key(s) {
    };
    bool hasNext() const;
    void next(std::string& k, char * bytesOut, size_t buf_size);
    bool operator()(const lcdf::Str &key, row_type *, threadinfo *);

  
  protected:
    std::string start, end;
    std::string cur_key;
    

};

class JSMasstree {

 public:
  JSMasstree();
  ~JSMasstree();
  int elements() const;
  void set(const char * key, const char * data, size_t data_size);
  void get(const char * key, char * data, size_t& data_size) const;  
  void clear();
  
  JSMScanner scan(const std::string& start, const std::string& end) const;
  
 protected:
  mt_table * masstree_table;
  JSMasstree(const JSMasstree&) {}
  JSMasstree& operator=(const JSMasstree& ) {return *this;}
//  get(const char* )

   void set_ptr(const char * key, void * data);
   void * get_ptr(const char * key) const;

};


template <class P>
class JSMasstreePtr: public JSMasstree {
 public:
   void set(const char * key, const P * data) {
    set_ptr(key, (void*) data);
   }  
   P* get(const char * key) const {
    return (P*) get_ptr(key);
   }

};

template<class RV>
class JSMCursor {

  public:
    bool has_next();
    std::pair<char *, RV* > next(size_t& sz);
    JSMCursor(const JSMasstreePtr<RV>& t, const char* start_key, const char * end_key);

  private:

};

#endif
