#include <glog/logging.h>
#include "dataplane_comm.h"

using namespace jetstream;
using namespace std;
using namespace boost;
using namespace boost::asio::ip;



void
IncomingConnectionState::got_data_cb (const DataplaneMessage &msg,
                                   const boost::system::error_code &error) {
  if (error) {
    if (error != boost::system::errc::operation_canceled) 
      LOG(WARNING) << "error trying to read data: " << error.message();
    if( dest ) {
      dest->no_more_tuples();  //tear down chain
      conn->close_async(boost::bind(&DataplaneConnManager::cleanup_incoming, &mgr, conn->get_remote_endpoint()));
    }
    return;
  }
  
  if (!dest)
    LOG(FATAL) << "got data but no operator to receive it";

  switch (msg.type ()) {
  case DataplaneMessage::DATA:
    {
      if (mon->is_congested()) {
        VLOG(1) << "reporting upstream congestion at " << dest->id_as_str();
        report_congestion_upstream(1);
        register_congestion_recheck();
      }
//      LOG(INFO) << "GOT DATA; length is " << msg.data_size() << "tuples";
      for(int i=0; i < msg.data_size(); ++i) {
        shared_ptr<Tuple> data (new Tuple);
        data->MergeFrom (msg.data(i));
        assert (data->e_size() > 0);

        dest->process(data, remote_op);
      }
      DataplaneMessage resp;
      resp.set_type(DataplaneMessage::ACK);
      resp.set_bytes_processed(msg.ByteSize());
      boost::system::error_code err;
      
      conn->send_msg(resp, err);
      break;
    }
  case DataplaneMessage::NO_MORE_DATA:
    {
      LOG(INFO) << "got no-more-data signal from " << conn->get_remote_endpoint()
                << ", will tear down connection into " << dest->id_as_str();
      conn->close_async(boost::bind(&DataplaneConnManager::cleanup_incoming, &mgr, conn->get_remote_endpoint()));

    }
    break;
  case DataplaneMessage::TS_ECHO:
    {
      LOG(INFO)  << "got ts echo; responding";
      boost::system::error_code err;
      conn->send_msg(msg, err); // just echo back what we got
    }
    break;
  
  
  default:
//      LOG(WARNING) << "unexpected dataplane message: "<<msg.type() <<  " from "
//                   << conn->get_remote_endpoint() << " for existing dataplane connection";
      //fall through
//  case DataplaneMessage::SET_BACKOFF:
//  case DataplaneMessage::CONGEST_STATUS:
//  case DataplaneMessage::END_OF_WINDOW:
    {
      dest->meta_from_upstream(msg, remote_op);
    }
    break;

  }

  // Wait for the next data message
  boost::system::error_code e;
  conn->recv_data_msg(bind(&IncomingConnectionState::got_data_cb,
  			this, _1, _2), e);
}
  

void
IncomingConnectionState::report_congestion_upstream(double level) {
  DataplaneMessage msg;
  msg.set_type(DataplaneMessage::CONGEST_STATUS);
  msg.set_congestion_level(level);

  VLOG(1) << "Reporting congestion at " << dest->id_as_str()<< ": " << level;
  boost::system::error_code error;
  conn->send_msg(msg, error);
  if (error) {
    LOG(WARNING) << "Failed to report congestion: " << error.message();
    if (dest)
      dest->no_more_tuples();
    conn->close_async(boost::bind(&DataplaneConnManager::cleanup_incoming, &mgr, conn->get_remote_endpoint()));
  }
}


void
IncomingConnectionState::register_congestion_recheck() {
  timer.expires_from_now(boost::posix_time::millisec(100));
  timer.async_wait(boost::bind(&IncomingConnectionState::congestion_recheck_cb, this, _1));
}


void
IncomingConnectionState::congestion_recheck_cb(const boost::system::error_code& error) {
  if (error == boost::asio::error::operation_aborted)
    return;

  VLOG(1) << "rechecking congestion at "<< dest->id_as_str();
  double level = mon->capacity_ratio();
  if (conn->is_connected()) {
    report_congestion_upstream(level);
//  if (!mon->is_congested()) // only report if congestion went away
//    report_congestion_upstream(0);
//  else
    register_congestion_recheck();
  }
}



void
IncomingConnectionState::meta_from_downstream(const DataplaneMessage & msg) {
  if (conn->is_connected()) {
//    LOG(INFO) << "propagating meta downstream";
    boost::system::error_code error;
    conn->send_msg(msg, error);
    if (error) {
      LOG(WARNING) << "Failed to send message upwards: " << error.message();
      if (dest)
        dest->no_more_tuples();
      conn->close_async(boost::bind(&DataplaneConnManager::cleanup_incoming, &mgr, conn->get_remote_endpoint()));
    }
  }
}

void
DataplaneConnManager::enable_connection (shared_ptr<ClientConnection> c,
                                         shared_ptr<TupleReceiver> dest,
                                         operator_id_t srcOpID) {
  
  boost::shared_ptr<IncomingConnectionState> incomingConn;
  {
    lock_guard<boost::recursive_mutex> lock (incomingMapMutex);

    if (liveConns.find(c->get_remote_endpoint()) != liveConns.end()) {
      //TODO this can probably happen if the previous connection died. Can we check for that?
      LOG(FATAL) << "Trying to connect remote conn from "<< c->get_remote_endpoint()
                 << " to " << dest->id_as_str() << "but there already is a connection";
    }
    incomingConn = boost::shared_ptr<IncomingConnectionState> (
          new IncomingConnectionState(c, dest, iosrv, *this, srcOpID));
    liveConns[c->get_remote_endpoint()] = incomingConn;
    dest->add_pred(incomingConn);
    
  }
  
  boost::system::error_code error;
  c->recv_data_msg(bind(&IncomingConnectionState::got_data_cb,
			incomingConn,  _1, _2), error);

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
                                          string future_op,
                                          operator_id_t srcOpID) {
/*  if (pendingConns.find(future_op) != pendingConns.end()) {
    //TODO this can probably happen if the previous connection died. Can we check for that?
    LOG(FATAL) << "trying to connect remote conn to " << future_op << "but there already is such a connection";
  }*/
  lock_guard<boost::recursive_mutex> lock (incomingMapMutex);

  
  pendingConns[future_op].push_back( PendingConn(c, srcOpID) );
}


// Called whenever an operator is created.
void
DataplaneConnManager::created_operator (shared_ptr<TupleReceiver> dest) {

  {
    lock_guard<boost::recursive_mutex> lock (incomingMapMutex);
    string op_id = dest->id_as_str();
    map<string, vector<PendingConn> >::iterator pending_conn = pendingConns.find(op_id);
    if (pending_conn != pendingConns.end()) {
      vector< PendingConn > & conns = pending_conn->second;
      for (u_int i = 0; i < conns.size(); ++i)
        enable_connection(conns[i].conn, dest, conns[i].src);
      pendingConns.erase(pending_conn);
    }
  }
}
void
DataplaneConnManager::close() {
  lock_guard<boost::recursive_mutex> lock (incomingMapMutex);

  //TODO: gracefully stop connections
  std::map<std::string, vector< PendingConn > >::iterator iter;

  for (iter = pendingConns.begin(); iter != pendingConns.end(); iter++) {
    vector<PendingConn> & conns = iter->second;
    for (u_int i = 0; i < conns.size(); ++i) {
      LOG(INFO) << "In dataplane teardown, disconnecting pending conn " << conns[i].conn->get_remote_endpoint();
      conns[i].conn->close_now();
    }
  }

  std::map<tcp::endpoint, boost::shared_ptr<IncomingConnectionState> >::iterator live_iter;

  for (live_iter = liveConns.begin(); live_iter != liveConns.end(); live_iter++) {
    LOG(INFO) << "In dataplane teardown, disconnecting from " << live_iter->second->get_remote_endpoint();

    live_iter->second->close_now();
  }
  
  //TODO stop RDAs?
  VLOG(1) << "Dataplane communications are closed";
}
  

RemoteDestAdaptor::RemoteDestAdaptor (DataplaneConnManager &dcm,
                                      ConnectionManager &cm,
                                      boost::asio::io_service & io,
                                      const Edge &e,
                                      msec_t wait,
                                      boost::shared_ptr<TupleSender> p)
  : mgr(dcm), iosrv(io), chainIsReady(false), this_buf_size(0),
    timer(io), wait_for_conn(wait) {
  pred = p;
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
    dest_as_str = dest_as_edge.dest_cube();
  }
  
  cm.create_connection(remoteAddr, portno, boost::bind(
                 &RemoteDestAdaptor::conn_created_cb, this, _1, _2));
      
}

void
RemoteDestAdaptor::conn_created_cb(shared_ptr<ClientConnection> c,
                                     boost::system::error_code error) {
  if (error || ! c->is_connected()) {
    LOG(WARNING) << "Dataplane connection failed: " << error.message();
    return;
  }

  conn = c;

  DataplaneMessage data_msg;
  data_msg.set_type(DataplaneMessage::CHAIN_CONNECT);
  
  std::ostringstream mon_name;
  mon_name << "connection to " << c->get_remote_endpoint();

  remote_processing = boost::shared_ptr<QueueCongestionMonitor>(
    new QueueCongestionMonitor(mgr.maxQueueSize(), mon_name.str(), dest_as_edge.max_kb_per_sec() * 1000));

//  conn->congestion_monitor()->set_queue_size(mgr.maxQueueSize());
//  if(dest_as_edge.has_max_kb_per_sec())
//    conn->congestion_monitor()->set_max_rate(dest_as_edge.max_kb_per_sec() * 1000); //convert kb --> bytes

  Edge * edge = data_msg.mutable_chain_link();
  edge->CopyFrom(dest_as_edge);
  
  
  boost::system::error_code err;
  conn->recv_data_msg(boost::bind(&RemoteDestAdaptor::conn_ready_cb, 
				  this, _1, _2), err);
  conn->send_msg(data_msg, err);
}

void
RemoteDestAdaptor::conn_ready_cb(const DataplaneMessage &msg,
                                        const boost::system::error_code &error) {

  if (error) {
    if (error != boost::system::errc::operation_canceled) 
      LOG(WARNING) << error.message() << " code = " << error.value();
    
    //FIXME need to tear down?
    return;
  }

  switch (msg.type ()) {
    case DataplaneMessage::CHAIN_READY:
    {
      LOG(INFO) << "got ready back from " << dest_as_str;
      {
        unique_lock<boost::mutex> lock(mutex);
        // Indicate the chain is ready before calling notify to avoid a race condition
        chainIsReady = true;
      }
      // Unblock any threads that are waiting for the chain to be ready; the mutex does
      // not need to be locked across the notify call
      chainReadyCond.notify_all();  
      break;
    }
    
    case DataplaneMessage::CONGEST_STATUS:
    {
      double status = msg.congestion_level();
      VLOG(1) << "Received remote congestion report from " <<  dest_as_str <<" : status is " << status;

      remote_processing->set_upstream_congestion(status);
      break;
    }

    case DataplaneMessage::SET_BACKOFF:
    {
      pred->meta_from_downstream(msg);
      break;
    }

    case DataplaneMessage::ACK:
    {
      remote_processing->report_delete(0, msg.bytes_processed());
      break;
    }
  
    default:
      LOG(WARNING) << "unexpected incoming message after chain connect: " << msg.Utf8DebugString()
                   << std::endl << "Error code is " << error;
  }

  boost::system::error_code err;  
  conn->recv_data_msg(bind(&RemoteDestAdaptor::conn_ready_cb,
  			this, _1, _2), err);

}

  
void
RemoteDestAdaptor::process (boost::shared_ptr<Tuple> t, const operator_id_t src) {
  if (!wait_for_chain_ready()) {
    LOG(WARNING) << "timeout on dataplane connection to " << dest_as_str
		 << ". Aborting data message send. Should queue/retry instead?";
    return;
  }

  {
    unique_lock<boost::mutex> lock(mutex);
    msg.set_type(DataplaneMessage::DATA);
    
    bool buffer_was_empty = (this_buf_size == 0);
    this_buf_size += t->ByteSize();
    msg.add_data()->MergeFrom(*t);
    
    if (this_buf_size < SIZE_TO_SEND) {
      if (buffer_was_empty) {
        timer.expires_from_now(boost::posix_time::millisec(WAIT_FOR_DATA));
        timer.async_wait(boost::bind(&RemoteDestAdaptor::force_send, this));
      }
      return;
    } else
      do_send_unlocked();
  }
}


void
RemoteDestAdaptor::force_send() {
  unique_lock<boost::mutex> lock(mutex);
  if (this_buf_size == 0)
    return;
  do_send_unlocked();
}

void
RemoteDestAdaptor::do_send_unlocked() {

  this_buf_size = 0;
  timer.cancel();
  
  boost::system::error_code err;
  int sent = conn->send_msg(msg, err);
  msg.Clear();
  
  if (err != 0) {
    //send failed
    LOG(WARNING) << "Send failed; tearing down chain; local end is " << pred->id_as_str();
    pred->chain_is_broken();
    conn->close_async(boost::bind(&DataplaneConnManager::cleanup, &mgr, dest_as_str));
  } else {
    remote_processing->report_insert(0, sent);
  }
  
}

void
RemoteDestAdaptor::no_more_tuples () {
  if (!wait_for_chain_ready()) {
    LOG(WARNING) << "timeout on dataplane connection to "<< dest_as_str
		 << ". Aborting no-more-data message send. Should queue/retry instead?";
    return;
  }
  
  force_send();

  DataplaneMessage d;
  d.set_type(DataplaneMessage::NO_MORE_DATA);
  
  boost::system::error_code err;
  conn->send_msg(d, err);
  VLOG(1) << "sent last tuples from connection (total is " << conn->send_count() << "); waiting.";
  
  conn->close_async(boost::bind(&DataplaneConnManager::cleanup, &mgr, dest_as_str));
  //need to wait until the close actually happens before returning, to avoid
  //destructing while the connection is in use.
  
  //TODO should clean self up.
//  mgr.deferred_cleanup(remoteAddr); //do this synchronously
}


void
RemoteDestAdaptor::meta_from_upstream(const DataplaneMessage & msg, const operator_id_t pred) {
  if (!wait_for_chain_ready()) {
    LOG(WARNING) << "timeout on dataplane connection to "<< dest_as_str
		 << ". Aborting meta message send. Should queue/retry instead?";
    return;
  }

  boost::system::error_code err;
  
  //we are on caller's thread so this is thread-safe.
  force_send(); 
  conn->send_msg(msg, err);
}

bool
RemoteDestAdaptor::wait_for_chain_ready() {
  unique_lock<boost::mutex> lock(mutex); // wraps mutex in an RIAA pattern
  while (!chainIsReady) {
    LOG(INFO) << "trying to send data to "<< dest_as_str << " on "
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
  lock_guard<boost::recursive_mutex> lock (outgoingMapMutex);
 
  shared_ptr<RemoteDestAdaptor> a = adaptors[id];
  if (!a) {
    LOG(ERROR) << "No record of adaptor " << id << ", optimistically assuming it's already destructed";
    return;
  }
  assert(a);
  
  if (!a->conn || !a->conn->is_connected()) {
    adaptors.erase(id);
  } else {
    LOG(FATAL) << "need to handle deferred cleanup of an in-use rda";
   //should set this up on a timer
  }
}

void
DataplaneConnManager::deferred_cleanup_in(tcp::endpoint e) {

  lock_guard<boost::recursive_mutex> lock (incomingMapMutex);
  liveConns.erase(e);
}



const std::string RemoteDestAdaptor::generic_name("Remote connection");

