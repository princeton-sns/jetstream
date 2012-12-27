import logging

import threading
import socket


logger = logging.getLogger('JetStream')


import BaseHTTPServer

SERV_PORT = 8082
def start_web_interface(js_server, endpt, port = SERV_PORT):
  web_interface = JSHttpServer(js_server, endpt, port)
  iface_thread = threading.Thread(group = None, target = web_interface.serve_forever, args = ())
  iface_thread.daemon = True
  iface_thread.start()

class JSHttpServer(BaseHTTPServer.HTTPServer):
  def __init__(self, js_server, endpt, port):
    self.js_server = js_server
    self.allow_reuse_address = True
    BaseHTTPServer.HTTPServer.__init__(self, (endpt, port), JSWebInterface)
    logger.info("Web interface started on port %d of: %s" % (port, endpt))


class  JSWebInterface(BaseHTTPServer.BaseHTTPRequestHandler):
  """A simple HTTP status page for the server.
    Ideally, the web interface would go only through the public interfaces of the
    Controller class, for thread safety.
    """
      
  def do_GET(self):
    print "received GET"
    self.send_response(200)
    self.send_header("Content-type", "text/html")
    self.end_headers()
    self.wfile.write("<html><head><title>JetStream Status.</title></head>")
    self.wfile.write("<body>")
    self.print_node_list()
    self.wfile.write("<hr/>")    
    self.print_computation_list()
    self.wfile.write("<hr/>")
    self.print_cube_list()

    self.wfile.write("</body></html>")
    
    
  def print_node_list(self):
    nodes = self.server.js_server.get_nodes()
    self.wfile.write("<p>Total of %d nodes. </p><ul>" % len(nodes))
    for n in nodes: 
      self.wfile.write("<li>%s:%d</li>" % n.get_dataplane_ep())
    self.wfile.write("</ul>")

  def print_computation_list(self):

    computations = self.server.js_server.computations
    self.wfile.write("<p>Total of %d computations.</p> <ul>" % len(computations))
    for c,_ in computations.items():  #TODO thread safety?
      self.wfile.write("<li>%d</li>" % (c))
    self.wfile.write("</ul>")

  def print_cube_list(self):
    cubes = self.server.js_server.get_cubes() #thread safe
    self.wfile.write("<p>Total of %d known cubes. </p> <ul>" % len(cubes))
    for cube_name, cube_location in cubes:
      if cube_location is not None:
        self.wfile.write("<li>Cube %s on node %s</li>" % (cube_name, str(cube_location)))
      else:
        self.wfile.write("<li>Cube %s is currently inaccessible.</li>")
    self.wfile.write("</ul>")

