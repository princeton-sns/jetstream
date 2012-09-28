#include <glog/logging.h>
#include "dataplane_comm.h"

using namespace jetstream;
using namespace std;
using namespace boost;


void
DataplaneConnManager::enable_connection (shared_ptr<ClientConnection> c,
                                         operator_id_t dest_op_id,
                                         shared_ptr<DataPlaneOperator> dest) {
  liveConns[dest_op_id] = c;

  boost::system::error_code error;
  c->recv_data_msg(bind(&DataplaneConnManager::got_data_cb,
			this, dest_op_id, dest,  _1, _2), error);

  DataplaneMessage response;
  if (!error) {
    LOG(INFO) << "created dataplane connection into " << dest->id();
    response.set_type(DataplaneMessage::CHAIN_READY);
    // XXX This should include an Edge
  }
  else {
    LOG(WARNING) << " couldn't enable receive-data callback; "<< error;
  }
  
  c->send_msg(response, error);
}
                
     
void
DataplaneConnManager::pending_connection (shared_ptr<ClientConnection> c,
                                          operator_id_t future_op) {
  pendingConns[future_op] = c;
}


// Calle
void
DataplaneConnManager::created_operator (operator_id_t op_id,
                                        shared_ptr<DataPlaneOperator> dest) {
  shared_ptr<ClientConnection> c = pendingConns[op_id];
  pendingConns.erase(op_id);
  if (c != NULL)
    enable_connection(c, op_id, dest);
}

void
DataplaneConnManager::got_data_cb (operator_id_t dest_op_id,
                                   shared_ptr<DataPlaneOperator> dest,
                                   const DataplaneMessage &msg,
                                   const boost::system::error_code &error) {
  if (error) {
    LOG(WARNING) << "error trying to read data: " << error.message();
    return;
  }
  
  if (!dest)
    LOG(FATAL) << "got data but no operator to receive it";

  shared_ptr<ClientConnection> c = liveConns[dest_op_id];
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
    default:
      LOG(WARNING) << "unexpected dataplane message: "<<msg.type() << 
        " from " << c->get_remote_endpoint() << " for existing dataplane connection";
  }

  // Wait for the next data message
  boost::system::error_code e;
  c->recv_data_msg(bind(&DataplaneConnManager::got_data_cb,
  			this, dest_op_id, dest, _1, _2), e);
}
  
  
void
DataplaneConnManager::close() {
  //TODO: gracefully stop connections
  std::map<operator_id_t, boost::shared_ptr<ClientConnection> >::iterator iter;

  for (iter = pendingConns.begin(); iter != pendingConns.end(); iter++) {
    iter->second->close();
  }
  for (iter = liveConns.begin(); iter != liveConns.end(); iter++) {
    iter->second->close();
  }
}
  

RemoteDestAdaptor::RemoteDestAdaptor (ConnectionManager& cm,
                                          const Edge & e) 
  : chainIsReady(false) {
                                          
  remoteAddr = e.dest_addr().address();
  int32_t portno = e.dest_addr().portno();      
  destOpId.computation_id = e.computation();
  destOpId.task_id = e.dest();
                                          
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
  edge->set_computation(destOpId.computation_id);
  edge->set_dest(destOpId.task_id);
  edge->set_src(0);
  
  boost::system::error_code err;
  conn->recv_data_msg(boost::bind( &RemoteDestAdaptor::conn_ready_cb, 
           this, _1, _2), err);
  conn->send_msg(data_msg, err);
}

void
RemoteDestAdaptor::conn_ready_cb(const DataplaneMessage &msg,
                                        const boost::system::error_code &error) {

  if (msg.type() == DataplaneMessage::CHAIN_READY) {
    LOG(INFO) << "got ready back";
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
    LOG(WARNING) << "unexpected response to Chain connect: " << msg.Utf8DebugString() <<
       "\nError code is " << error;  
  }
}

  
void
RemoteDestAdaptor::process (boost::shared_ptr<Tuple> t) 
{
  {
    unique_lock<boost::mutex> lock(mutex); // wraps mutex in an RIAA pattern
    while (!chainIsReady) {
      LOG(WARNING) << "trying to send data through closed conn. Should block";
   
      system_time wait_until = get_system_time()+ posix_time::milliseconds(wait_for_conn);
      bool conn_established = chainReadyCond.timed_wait(lock, wait_until);
      
//      if (stopping)
//         return;
      if (!conn_established) {
        LOG(WARNING) << "timeout on dataplane connection. Aborting for this tuple. Should queue/retry instead?";
        return;
      } 
    }
  }

  DataplaneMessage d;
  d.set_type(DataplaneMessage::DATA);
  d.add_data()->MergeFrom(*t);
  //TODO: could we merge multiple tuples here?

  boost::system::error_code err;
  conn->send_msg(d, err);
}
  

string
RemoteDestAdaptor::as_string() {
    std::ostringstream buf;
    buf << "connection to " << destOpId << " on " << remoteAddr <<
       (chainIsReady ? " (ready)" : " (unready)");
    return buf.str();
}