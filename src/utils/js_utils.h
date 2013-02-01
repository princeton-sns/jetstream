#ifndef _JS_UTILS_H_
#define _JS_UTILS_H_

#include <string>
#include "js_defs.h"
#include "js_boost_defs.h"
#include "jetstream_types.pb.h"

namespace jetstream {
void         jetstream_init ();
void         refresh_time ();
sec_t        get_sec ();
usec_t       get_usec ();
msec_t       get_msec ();
timespec     get_time ();
int          js_usleep(useconds_t usecs);
// std::string  get_strtime ();


std::string fmt(const jetstream::Tuple& t);



struct operator_id_t {
  int32_t computation_id; // which computation
  int32_t task_id;        // which operator in the computation

  bool operator< (const operator_id_t& rhs) const {
    return computation_id < rhs.computation_id 
      || task_id < rhs.task_id;
  }

  bool operator== (const operator_id_t& rhs) const {
    return computation_id == rhs.computation_id
      && task_id == rhs.task_id;
  }
  
  bool operator!= (const operator_id_t& rhs) const {
    return !( *this == rhs);
  }
  
  std::string to_string () {
    std::ostringstream buf;
    buf << "(" << computation_id << "," << task_id << ")";
    return buf.str();
  }
    
  operator_id_t (int32_t comp, int32_t t) : computation_id (comp), task_id (t) {}
  operator_id_t () : computation_id (0), task_id (0) {}
};

inline std::ostream& operator<<(std::ostream& out, operator_id_t id) {
  out << "(" << id.computation_id << "," << id.task_id << ")";
  return out;
}


TaskMeta* 
add_operator_to_alter(AlterTopo& topo, operator_id_t dest_id, const std::string& name);

Edge * 
add_edge_to_alter(AlterTopo& topo, operator_id_t src_id, operator_id_t dest_id);


Edge * 
add_edge_to_alter(AlterTopo& topo, int group, int s, int d);

Edge * 
add_edge_to_alter(AlterTopo& topo, std::string src_id, operator_id_t dest_id);

Edge * 
add_edge_to_alter(AlterTopo& topo, operator_id_t src_id, std::string dest_id);


inline CubeMeta* add_cube_to_alter(AlterTopo& topo, std::string name, bool overwrite_old = true) {
  CubeMeta * m = topo.add_tocreate();
  m->set_name(name);
  m->set_overwrite_old(overwrite_old);
  return m;
}

void add_dimension(CubeMeta* m, CubeSchema_Dimension_DimensionType, const std::string& name, int idx);
void add_aggregate(CubeMeta* m, const std::string& agg_name, const std::string& name, int idx);


void add_cfg_to_task(TaskMeta*, std::string optname, std::string val);

inline void extend_tuple(jetstream::Tuple& t, int32_t i) {
  t.add_e()->set_i_val(i);
}
inline void extend_tuple(jetstream::Tuple& t, double d) {
  t.add_e()->set_d_val(d);
}
inline void extend_tuple(jetstream::Tuple& t, const std::string& s) {
  t.add_e()->set_s_val(s);
}
inline void extend_tuple_time(jetstream::Tuple& t, time_t time) {
  t.add_e()->set_t_val((int)time);
}

}

#endif /* _JS_UTILS_H_ */
