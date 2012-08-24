

#include "node.h"
#include <glog/logging.h>

using namespace ::jetstream;

void
NodeWebInterface::start()
{

  if (mongoose_ctxt != NULL) {
    LOG(FATAL) << "Trying to re-initialize initialized web server";
  }
  const char* mongoose_cfg[] = {"listening_ports", "8081", NULL};
  
  
  mongoose_ctxt = mg_start(process_req, this, mongoose_cfg);
}



void
NodeWebInterface::stop()
{
  if (mongoose_ctxt != NULL) {
    mg_stop(mongoose_ctxt); //turn off the web server
    mongoose_ctxt = NULL;
  }
}


const char * hdr = "HTTP/1.1 200 OK\r\n"
              "Content-Type: text/html\r\n"
              "Content-Length: %d\r\n"        // Always set Content-Length
              "\r\n";


void *
NodeWebInterface::process_req(enum mg_event event, struct mg_connection *conn)
{
  if (event == MG_NEW_REQUEST) {
    const mg_request_info* request = mg_get_request_info(conn);
    NodeWebInterface * web_iface_obj =
             reinterpret_cast<NodeWebInterface*> (request -> user_data);
             
    ostringstream response;    
    web_iface_obj->make_base_page(response);

    string s = response.str();
    mg_printf(conn, hdr, s.length());
    mg_write(conn, s.c_str(), s.length());
    
    return (void *) hdr;     // Mark as processed
  } //not a request, so ignore the callback and let Mongoose do the default thing.
  return NULL;
}


void
NodeWebInterface::make_base_page(ostream &buf)
{
  buf <<"<html><body>JetStream worker alive</body></html>";
}