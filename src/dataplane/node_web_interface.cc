#include <glog/logging.h>
#include "node.h"
#include "node_web_interface.h"

using namespace jetstream;
using namespace std;
using namespace boost;

const int MAX_ROWS_TO_SHOW = 10;

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
    //FIXME: iscube_iter safe for cfg to go out of scope here?
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
      //need to render whole response first so we know length
      
    mg_printf(conn, hdr, s.length());
    mg_write(conn, s.c_str(), s.length());
    return (void *) hdr;  // Mark as processed
  }
  else {
    // Not a request, so ignore the callback and let Mongoose do the default thing.
    return NULL;
  }
}


void
NodeWebInterface::make_base_page(ostream &buf)
{
  buf << "<html><title>JetStream Status</title>" << endl;
  buf << "<h2>JetStream worker alive</h2>"<< endl ;
  boost::shared_ptr<vector<string> > cubeList = node.cubeMgr.list_cubes();
  
  buf << "<p>Cubes:</p>" << endl<<"<ol>"<<endl;
  vector<string>::iterator cube_it;
  for (cube_it = cubeList->begin();cube_it != cubeList->end(); ++cube_it) {
   string& name = *cube_it;
   boost::shared_ptr<DataCube> cube = node.cubeMgr.get_cube(name);
   if (!cube)
    continue; //cube deleted since list created
    
   buf << "<li><b>" << name << "</b> " + cube->num_leaf_cells() << " cells." << endl;

      //could print schema here


   Tuple empty = cube->empty_tuple();
   cube::CubeIterator it = cube->slice_query
              (empty, empty, true, std::list<std::string>(), MAX_ROWS_TO_SHOW);
    
   if (it.numCells() > 0) {
     buf << "<table>" << endl;
     while (it != cube->end()) {
       buf << "<tr>";
       boost::shared_ptr<Tuple> t = *(it++);
       for (int v = 0; v < t->e_size(); ++ v) {
         buf << "<td>";
         const Element& cell = t->e(v);
         if (cell.has_i_val())
           buf << cell.i_val();
         else if (cell.has_s_val())
           buf << cell.s_val();
         else if (cell.has_d_val())
           buf << cell.d_val();
         else if (cell.has_t_val())
           buf << cell.t_val();
         
         buf << "</td>";
       }
       buf << "</tr>";
     }
     buf << "</table>" << endl;
   }
   buf << "</li>" << endl;
  }
  
  buf << "</li>"<<endl;

  buf << "<p>Operators:</p>" << endl<<"<ol>"<<endl;

//TODO lock around access to node structures?
  map<operator_id_t,shared_ptr<DataPlaneOperator> >::iterator oper_it;
  for (oper_it = node.operators.begin(); oper_it != node.operators.end(); ++oper_it) {
    const operator_id_t& o_id = oper_it->first;
    shared_ptr<DataPlaneOperator> op = oper_it->second;
    buf << "<li><b>"<< op ->get_type() << " " << o_id << "</b>" << endl;
    buf << op->get_dest()->as_string();
    buf << "</li>";
  }
  
  buf << "</ol>" << endl;
  buf << "</body></html>"<< endl;
}


