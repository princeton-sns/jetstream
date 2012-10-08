#include <glog/logging.h>
#include "dataplane_comm.h"

using namespace jetstream;
using namespace std;
using namespace boost;
using namespace boost::asio::ip;


void
DataplaneConnManager::enable_connection (shared_ptr<ClientConnection> c,
                                         shared_ptr<TupleReceiver> dest) {
  
  if (liveConns.find(c->get_remote_endpoint()) != liveConns.end()) {
    //TODO this can probably happen if the previous connection died. Can we check for that?
    LOG(FATAL) << "Trying to connect remote conn from "<< c->get_remote_endpoint()
               << " to " << dest->id_as_str() << "but there already is a connection";
  }
  liveConns[c->get_remote_endpoint()] = c;

  boost::system::error_code error;
  c->recv_data_msg(bind(&DataplaneConnManager::got_data_cb,
			this, c, dest,  _1, _2), error);

  DataplaneMessage response;
  if (!error) {
    LOG(INFO) << "Created dataplane connection into " << dest->id_as_str();
    response.set_type(DataplaneMessage::CHAIN_READY);
    // XXX This should include an Edge
  }
  else {
    LOG(WARNING) << "Couldn't enable receive-data callback; "<< error;
  }
  
  c->send_msg(response, error);
}
                
     
void
DataplaneConnManager::pending_connection (shared_ptr<ClientConnection> c,
                                          string future_op) {
/*  if (pendingConns.find(future_op) != pendingConns.end()) {
    //TODO this can probably happen if the previous connection died. Can we check for that?
    LOG(FATAL) << "trying to connect remote conn to " << future_op << "but there already is such a connection";
  }*/

  pendingConns[future_op].push_back(c);
}


// Called whenever an operator is created.
void
DataplaneConnManager::created_operator (shared_ptr<TupleReceiver> dest) {
  string op_id = dest->id_as_str();
  map<string, vector<shared_ptr<ClientConnection> > >::iterator pending_conn = pendingConns.find(op_id);
  if (pending_conn != pendingConns.end()) {
    vector<shared_ptr<ClientConnection> > & conns = pending_conn->second;
    for (u_int i = 0; i < conns.size(); ++i)
      enable_connection(conns[i], dest);
    pendingConns.erase(pending_conn);
  }
}

void
DataplaneConnManager::got_data_cb (shared_ptr<ClientConnection> c,
                                   shared_ptr<TupleReceiver> dest,
                                   const DataplaneMessage &msg,
                                   const boost::system::error_code &error) {
  if (error) {
    LOG(WARNING) << "error trying to read data: " << error.message();
    return;
  }
  
  if (!dest)
    LOG(FATAL) << "got data but no operator to receive it";

  assert(c);

  switch (msg.type ()) {
  case DataplaneMessage::DATA:
    {
      shared_ptr<Tuple> data (new Tuple);
      for(int i=0; i < msg.data_size(); ++i) {
        data->MergeFrom (msg.data(i));
        dest->process(data);
      }
      break;
    }
  case DataplaneMessage::NO_MORE_DATA:
    {
      LOG(INFO) << "got no-more-data signal from " << c->get_remote_endpoint()
                << ", will tear down connection into " << dest->id_as_str();
      tcp::endpoint e = c->get_remote_endpoint();
      c->close();
      liveConns.erase(e);
    }
    break;
    
  default:
      LOG(WARNING) << "unexpected dataplane message: "<<msg.type() <<  " from " 
                   << c->get_remote_endpoint() << " for existing dataplane connection";
  }

  // Wait for the next data message
  boost::system::error_code e;
  c->recv_data_msg(bind(&DataplaneConnManager::got_data_cb,
  			this, c, dest, _1, _2), e);
}
  
  
void
DataplaneConnManager::close() {
  //TODO: gracefully stop connections
  std::map<std::string, vector< boost::shared_ptr<ClientConnection> > >::iterator iter;

  for (iter = pendingConns.begin(); iter != pendingConns.end(); iter++) {
    vector<shared_ptr<ClientConnection> > & conns = iter->second;
    for (u_int i = 0; i < conns.size(); ++i)
      conns[i]->close();
  }

  std::map<tcp::endpoint, boost::shared_ptr<ClientConnection> >::iterator live_iter;

  for (live_iter = liveConns.begin(); live_iter != liveConns.end(); live_iter++) {
    live_iter->second->close();
  }
}
  

RemoteDestAdaptor::RemoteDestAdaptor (DataplaneConnManager &dcm, ConnectionManager &cm,
                                          const Edge &e, msec_t wait)
  : mgr(dcm), chainIsReady(false), wait_for_conn(wait) {
                                          
  remoteAddr = e.dest_addr().address();
  int32_t portno = e.dest_addr().portno();
  
  dest_as_edge.CopyFrom(e);
  if (dest_as_edge.has_dest()) {
    operator_id_t destOpId;
    destOpId.computation_id = e.computation();
    destOpId.task_id = e.dest();
    dest_as_str = destOpId.to_string();
  }
  else {
    dest_as_str = dest_as_edge.cube_name();
  }
  
  cm.create_connection(remoteAddr, portno, boost::bind(
                 &RemoteDestAdaptor::conn_created_cb, this, _1, _2));
}

void
RemoteDestAdaptor::conn_created_cb(shared_ptr<ClientConnection> c,
                                     boost::system::error_code error) {
  conn = c;

  DataplaneMessage data_msg;
  data_msg.set_type(DataplaneMessage::CHAIN_CONNECT);
  
  Edge * edge = data_msg.mutable_chain_link();
  edge->CopyFrom(dest_as_edge);
  
  boost::system::error_code err;
  conn->recv_data_msg(boost::bind( &RemoteDestAdaptor::conn_ready_cb, 
           this, _1, _2), err);
  conn->send_msg(data_msg, err);
}

void
RemoteDestAdaptor::conn_ready_cb(const DataplaneMessage &msg,
                                        const boost::system::error_code &error) {

  if (msg.type() == DataplaneMessage::CHAIN_READY) {
    LOG(INFO) << "got ready back from " << dest_as_str;
    {
      unique_lock<boost::mutex> lock(mutex);
      // Indicate the chain is ready before calling notify to avoid a race condition
      chainIsReady = true;
    }
    // Unblock any threads that are waiting for the chain to be ready; the mutex does
    // not need to be locked across the notify call
    chainReadyCond.notify_all();  
  } 
  else {
    LOG(WARNING) << "unexpected response to Chain connect: " << msg.Utf8DebugString()
                 << std::endl << "Error code is " << error;
  }
}

  
void
RemoteDestAdaptor::process (boost::shared_ptr<Tuple> t) {
  if (!wait_for_chain_ready()) {
    LOG(WARNING) << "timeout on dataplane connection to "<< dest_as_str
		 << ". Aborting data message send. Should queue/retry instead?";
    return;
  }

  DataplaneMessage d;
  d.set_type(DataplaneMessage::DATA);
  d.add_data()->MergeFrom(*t);
  //TODO: could we merge multiple tuples here?

  boost::system::error_code err;
  conn->send_msg(d, err);
}


void
RemoteDestAdaptor::no_more_tuples () {
  if (!wait_for_chain_ready()) {
    LOG(WARNING) << "timeout on dataplane connection to "<< dest_as_str
		 << ". Aborting no-more-data message send. Should queue/retry instead?";
    return;
  }

  DataplaneMessage d;
  d.set_type(DataplaneMessage::NO_MORE_DATA);
  
  boost::system::error_code err;
  conn->send_msg(d, err);
  
  //TODO should clean self up.
}


bool
RemoteDestAdaptor::wait_for_chain_ready() {
  unique_lock<boost::mutex> lock(mutex); // wraps mutex in an RIAA pattern
  while (!chainIsReady) {
    LOG(WARNING) << "trying to send data to "<< dest_as_str << " on "
		 << remoteAddr << " through closed conn. Should block";
    
    system_time wait_until = get_system_time()+ posix_time::milliseconds(wait_for_conn);
    bool conn_established = chainReadyCond.timed_wait(lock, wait_until);
    
    //      if (stopping)
    //         return;
    if (!conn_established) {
      return false;
    } 
  }
  return true;
}


string
RemoteDestAdaptor::long_description() {
    std::ostringstream buf;
    buf << dest_as_str << " on " << remoteAddr <<
       (chainIsReady ? " (ready)" : " (waiting for dest)");
    return buf.str();
}


void
DataplaneConnManager::deferred_cleanup(string id) {
  shared_ptr<RemoteDestAdaptor> a = adaptors[id];
  assert(a);
  
  if (!a->conn->is_connected()) {
    adaptors.erase(id);
  } else {
    LOG(FATAL) << "need to handle deferred cleanup of an in-use rda";
   //should set this up on a timer
  
  }

}


const std::string RemoteDestAdaptor::generic_name("Remote connection");

