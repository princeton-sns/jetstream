#include <glog/logging.h>
#include "node.h"
#include "node_web_interface.h"

using namespace jetstream;
using namespace std;


NodeWebInterface::NodeWebInterface (port_t webPortno, Node &n) 
  : mongoose_ctxt (NULL),
    portno (webPortno),
    node (n)
{
}


void
NodeWebInterface::start ()
{
  if (mongoose_ctxt != NULL) {
    LOG(ERROR) << "Web server already initialized" << endl;
  }
  else {
    string port_as_str(boost::lexical_cast<string> (portno));
    const char *mg_config[] = {"listening_ports", port_as_str.c_str(), NULL};
    mongoose_ctxt = mg_start(process_req, this, mg_config);
    //FIXME: is it safe for cfg to go out of scope here?
  }
}

void
NodeWebInterface::stop ()
{
  if (mongoose_ctxt != NULL) {
    // Turn off the web server
    mg_stop(mongoose_ctxt); 
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
             reinterpret_cast<NodeWebInterface*> (request->user_data);
             
    std::ostringstream response;    
    web_iface_obj->make_base_page(response);

    std::string s = response.str();
    mg_printf(conn, hdr, s.length());
    mg_write(conn, s.c_str(), s.length());
    return (void *) hdr;  // Mark as processed
  } // Not a request, so ignore the callback and let Mongoose do the default thing.
  return NULL;
}


void
NodeWebInterface::make_base_page(ostream &buf)
{
  buf <<"<html><body>JetStream worker alive</body></html>";
}


