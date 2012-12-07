#ifndef _node_web_interface_H_
#define _node_web_interface_H_

#include <glog/logging.h>
#include "js_utils.h"
#include "mongoose.h"

namespace jetstream {

class Node;

class NodeWebInterface {
 private:
  mg_context *mongoose_ctxt;
  port_t portno;
  std::string bind_addr;
  Node &node;
  
  void make_base_page (std::ostream &buf);
  
  void operator= (const NodeWebInterface &) 
    { LOG(FATAL) << "cannot copy a ~NodeWebInterface"; }
  NodeWebInterface (const NodeWebInterface & n):node(n.node)
    { LOG(FATAL) << "cannot copy a ~NodeWebInterface"; }
  
 public:
  NodeWebInterface (const std::string& addr, port_t webPortno, Node &n);
  ~NodeWebInterface() { stop(); }
 
  void start ();
  void stop ();  // Idempotent, but may block to join with worker threads.
  
  static void * process_req (enum mg_event event, struct mg_connection *conn);
};

}

#endif
