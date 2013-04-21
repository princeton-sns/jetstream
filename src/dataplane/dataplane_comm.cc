#include <glog/logging.h>
#include "dataplane_comm.h"

using namespace jetstream;
using namespace std;
using namespace boost;
using namespace boost::asio::ip;

#undef REPORT_BW


void
IncomingConnectionState::no_more_tuples() {
  if (!dest)
    return;
  DataplaneMessage teardown;
  teardown.set_type(DataplaneMessage::NO_MORE_DATA);
  dest->meta_from_upstream(teardown);
}

void
IncomingConnectionState::got_data_cb (DataplaneMessage &msg,
                                   const boost::system::error_code &error) {
  if (error) {
    if (error != boost::system::errc::operation_canceled) 
      LOG(WARNING) << "error trying to read data: " << error.message();
    if( dest ) {
      no_more_tuples();  //tear down chain
      conn->close_async(boost::bind(&DataplaneConnManager::cleanup_incoming, &mgr, conn->get_remote_endpoint()));
    }
    return;
  }
  
  if (!dest)
    LOG(FATAL) << "got data but no operator to receive it";

  vector< shared_ptr<Tuple> > data;
  for(int i=0; i < msg.data_size(); ++i) {
    shared_ptr<Tuple> d (new Tuple);
    d->MergeFrom (msg.data(i));
    assert (d->e_size() > 0);
    data.push_back(d);

  }

  switch (msg.type ()) {
  case DataplaneMessage::DATA:
    {
/* FIXME CHAINS
      if (mon->is_congested()) {
        VLOG(1) << "reporting downstream congestion at " << dest->id_as_str();
        report_congestion_upstream(mon->capacity_ratio());
        register_congestion_recheck();
      }*/
//      LOG(INFO) << "GOT DATA; length is " << msg.data_size() << "tuples";

      dest->process(data, msg);
      
      /* FIXME CHAINS
      for (int i = 0; i < msg.old_val_size(); ++i) {
        shared_ptr<Tuple> new_data (new Tuple);
        new_data->MergeFrom (msg.new_val(i));
        Tuple old_data;
        old_data.MergeFrom(msg.old_val(i));
        dest->process_delta(old_data, new_data, remote_op);      
      }*/
#ifdef ACK_EACH_PACKET
      DataplaneMessage resp;
      resp.set_type(DataplaneMessage::ACK);
      resp.set_bytes_processed(msg.ByteSize());
      boost::system::error_code err;
      conn->send_msg(resp, err);
#endif
      break;
    }
  case DataplaneMessage::NO_MORE_DATA:
    {
      LOG(INFO) << "got no-more-data signal from " << conn->get_remote_endpoint()
                << ", will tear down connection into " << dest->chain_name();
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

  case DataplaneMessage::END_OF_WINDOW:
    {
      dest->meta_from_upstream(msg); //note that msg is a const param; can't mutate
#ifdef ACK_WINDOW_END
      LOG_EVERY_N(INFO, 40) << " got an end-of-window marker, acking it; ts was " << msg.timestamp()
       << " and window size was " << msg.window_length_ms();
      DataplaneMessage resp;
      resp.set_type(DataplaneMessage::ACK);
      resp.set_window_length_ms(msg.window_length_ms());
      resp.set_timestamp(msg.timestamp());
      
      boost::system::error_code err;
      conn->send_msg(resp, err); // just echo back what we got
#endif
      break;
    }
  
  default:
//      LOG(WARNING) << "unexpected dataplane message: "<<msg.type() <<  " from "
//                   << conn->get_remote_endpoint() << " for existing dataplane connection";
      //fall through
//  case DataplaneMessage::SET_BACKOFF:
//  case DataplaneMessage::CONGEST_STATUS:
//  case DataplaneMessage::END_OF_WINDOW:
    {
      dest->meta_from_upstream(msg);
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

  VLOG(1) << "Reporting congestion at " << dest->chain_name()<< ": " << level;
  boost::system::error_code error;
  conn->send_msg(msg, error);
  if (error) {
    LOG(WARNING) << "Failed to report congestion: " << error.message();
    no_more_tuples();
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

/* FIXME CHAINS
  VLOG(1) << "rechecking congestion at "<< dest->id_as_str();
  double level = mon->capacity_ratio();
  if (conn->is_connected()) {
    report_congestion_upstream(level);
//  if (!mon->is_congested()) // only report if congestion went away
//    report_congestion_upstream(0);
//  else
    register_congestion_recheck();
  }*/
}


/*
void
IncomingConnectionState::meta_from_downstream(const DataplaneMessage & msg) {
  if (conn->is_connected()) {
//    LOG(INFO) << "propagating meta downstream";
    boost::system::error_code error;
    conn->send_msg(msg, error);
    if (error) {
      LOG(WARNING) << "Failed to send message upwards: " << error.message();
      no_more_tuples();
      conn->close_async(boost::bind(&DataplaneConnManager::cleanup_incoming, &mgr, conn->get_remote_endpoint()));
    }
  }
}*/

void
DataplaneConnManager::enable_connection (shared_ptr<ClientConnection> c,
                                         shared_ptr<OperatorChain> dest,
                                         operator_id_t srcOpID) {
  
  boost::shared_ptr<IncomingConnectionState> incomingConn;
  {
    lock_guard<boost::recursive_mutex> lock (incomingMapMutex);

    if (liveConns.find(c->get_remote_endpoint()) != liveConns.end()) {
      //TODO this can probably happen if the previous connection died. Can we check for that?
      LOG(FATAL) << "Trying to connect remote conn from "<< c->get_remote_endpoint()
                 << " to " << dest->chain_name() << "but there already is a connection";
    }
    incomingConn = boost::shared_ptr<IncomingConnectionState> (
          new IncomingConnectionState(c, dest, iosrv, *this, srcOpID));
    liveConns[c->get_remote_endpoint()] = incomingConn;
//    dest->add_pred(incomingConn);
  }
  
  boost::system::error_code error;
  c->recv_data_msg(bind(&IncomingConnectionState::got_data_cb,
			incomingConn,  _1, _2), error);

  DataplaneMessage response;
  if (!error) {
    LOG(INFO) << "Created dataplane connection into " << dest->chain_name();
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
DataplaneConnManager::created_operator (shared_ptr<OperatorChain> dest) {

  {
    lock_guard<boost::recursive_mutex> lock (incomingMapMutex);
    string op_id = dest->member(0)->id_as_str();
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
                                      msec_t wait)
  : mgr(dcm), chainIsReady(false), this_buf_size(0),
    timer(io), wait_for_conn(wait) {
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
  conn->set_counters(mgr.send_counter, mgr.recv_counter);

  DataplaneMessage data_msg;
  data_msg.set_type(DataplaneMessage::CHAIN_CONNECT);
  
  std::ostringstream mon_name;
  mon_name << "connection to " << c->get_remote_endpoint();

#ifdef ACK_EACH_PACKET
  remote_processing = boost::shared_ptr<QueueCongestionMonitor>(
    new QueueCongestionMonitor(mgr.maxQueueSize(), mon_name.str()));
#endif
#ifdef ACK_WINDOW_END
  remote_processing = boost::shared_ptr<WindowCongestionMonitor>(
    new WindowCongestionMonitor(mon_name.str()));
#endif
//  conn->congestion_monitor()->set_queue_size(mgr.maxQueueSize());
  if(dest_as_edge.has_max_kb_per_sec())
     remote_processing->set_max_rate(dest_as_edge.max_kb_per_sec() * 1000); //convert kb --> bytes

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

      remote_processing->set_downstream_congestion(status);
      break;
    }

    case DataplaneMessage::TPUT_START:
    case DataplaneMessage::TPUT_ROUND_2:
    case DataplaneMessage::TPUT_ROUND_3:
    case DataplaneMessage::SET_BACKOFF:
    {
//      pred->meta_from_downstream(msg);
      break;
    }

    case DataplaneMessage::ACK:
    {    
#ifdef ACK_EACH_PACKET
      remote_processing->report_delete(0, msg.bytes_processed());
#endif
#ifdef ACK_WINDOW_END
      remote_processing->end_of_window(msg.window_length_ms(), msg.timestamp());
      reporter.sending_a_tuple(0);
      //TODO should track time more carefully here to avoid confusion on overlapped windows
#endif
      break;
    }
  
    default:
      LOG(WARNING) << "unexpected incoming message after chain connect: " << msg.Utf8DebugString()
                   << std::endl << "Error code is " << error;
  }

  if (conn) { //might have failed during meta_from_downstream etc calls
    boost::system::error_code err;
    conn->recv_data_msg(bind(&RemoteDestAdaptor::conn_ready_cb,
          this, _1, _2), err);
  } else {
//    pred->chain_is_broken();
  }

}

  
void
RemoteDestAdaptor::process (boost::shared_ptr<Tuple> t, const operator_id_t src) {
  if (!wait_for_chain_ready()) {
    LOG(WARNING) << "timeout on dataplane connection to " << dest_as_str
		 << ". Aborting data message send. Should queue/retry instead?";
    return;
  }

  {
    size_t sz = t->ByteSize();
    reporter.sending_a_tuple(sz);
    remote_processing->report_insert(0, sz);

    unique_lock<boost::mutex> lock(mutex); //lock around buffers
    out_buffer_msg.set_type(DataplaneMessage::DATA);
    
    bool buffer_was_empty = (this_buf_size == 0);
    this_buf_size += sz;
    out_buffer_msg.add_data()->MergeFrom(*t);
    
    
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
RemoteDestAdaptor::process_delta (Tuple& oldV, boost::shared_ptr<Tuple> newV, const operator_id_t pred) {
  if (!wait_for_chain_ready()) {
    LOG(WARNING) << "timeout on dataplane connection to " << dest_as_str
		 << ". Aborting data message send. Should queue/retry instead?";
    return;
  }

  {
    size_t sz = oldV.ByteSize() + newV->ByteSize();
    reporter.sending_a_tuple(sz);
    remote_processing->report_insert(0, sz);

    unique_lock<boost::mutex> lock(mutex); //lock around buffers
    out_buffer_msg.set_type(DataplaneMessage::DATA); //TODO should be delta?
    
    bool buffer_was_empty = (this_buf_size == 0);
    this_buf_size += sz;
    out_buffer_msg.add_old_val()->MergeFrom(oldV);
    out_buffer_msg.add_new_val()->MergeFrom(*newV);
    
    
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
RemoteDestAdaptor::process ( OperatorChain * chain,
                             std::vector<boost::shared_ptr<Tuple> > & tuples,
                             DataplaneMessage& msg) {
  if (!wait_for_chain_ready()) {
    LOG(WARNING) << "timeout on dataplane connection to " << dest_as_str
		 << ". Aborting data message send. Should queue/retry instead?";
    return;
  }

  {
    unique_lock<boost::mutex> lock(mutex); //lock around buffers
    out_buffer_msg.set_type(DataplaneMessage::DATA);

    bool buffer_was_empty = (this_buf_size == 0);
    
    for(int i = 0; i < tuples.size(); ++i) {
      boost::shared_ptr<Tuple> t = tuples[i];
      if (!t)
        continue;
      size_t sz = t->ByteSize();
      reporter.sending_a_tuple(sz);
      remote_processing->report_insert(0, sz);
      this_buf_size += sz;
      out_buffer_msg.add_data()->MergeFrom(*t);
    }
    
    if (msg.has_type() || this_buf_size > SIZE_TO_SEND) {
      do_send_unlocked();
      meta_from_upstream(msg); 
    } else {
      if (buffer_was_empty) {
        timer.expires_from_now(boost::posix_time::millisec(WAIT_FOR_DATA));
        timer.async_wait(boost::bind(&RemoteDestAdaptor::force_send, this));
      }
    }
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
  conn->send_msg(out_buffer_msg, err);
  out_buffer_msg.Clear();
  
  if (err != 0) {
    //send failed
    LOG(WARNING) << "Send failed; tearing down chain"; //; local end is " << pred->id_as_str();
//    pred->chain_is_broken();
    conn->close_async(boost::bind(&DataplaneConnManager::cleanup, &mgr, dest_as_str));
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
RemoteDestAdaptor::meta_from_upstream(const DataplaneMessage & msg_in) {
  if (!wait_for_chain_ready()) {
    LOG(WARNING) << "timeout on dataplane connection to "<< dest_as_str
		 << ". Aborting meta message send. Should queue/retry instead?";
    return;
  }

  boost::system::error_code err;
  if( msg_in.type() == DataplaneMessage::NO_MORE_DATA) {
    no_more_tuples();
    return;
  }
#ifdef ACK_WINDOW_END
  else if (msg_in.type() == DataplaneMessage::END_OF_WINDOW &&
      msg_in.has_window_length_ms()) {
    if (remote_processing->get_window_start() > 0) { //data in window
      DataplaneMessage msg_out;
      msg_out.CopyFrom(msg_in);
      msg_out.set_timestamp(  remote_processing->get_window_start()  );
//      LOG(INFO) << "Sending out end-of-window. window start ts = " << msg_out.timestamp();
      //we are on caller's thread so this is thread-safe.
      force_send(); 
      conn->send_msg(msg_out, err);
      remote_processing->new_window_start(); //reset clock
    } else {
      LOG(INFO) << "end of window with no data";
      remote_processing->end_of_window(msg_in.window_length_ms(), 0);
      force_send();
      conn->send_msg(msg_in, err);      
    }
  }
#endif
  else {
    //we are on caller's thread so this is thread-safe.
    force_send(); 
    conn->send_msg(msg_in, err);
  }
}

bool
RemoteDestAdaptor::wait_for_chain_ready() {
  unique_lock<boost::mutex> lock(mutex); // wraps mutex in an RIAA pattern
  while (!chainIsReady) {
    LOG(INFO) <<  /*pred->id_as_str() << */ " trying to send data to "<< dest_as_str << " on "
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



void
BWReporter::sending_a_tuple(size_t b) {
  tuples ++;
  bytes += b;
  msec_t now = get_msec();
  if ( now > next_report) {
#ifdef REPORT_BW
    time_t last_report = next_report - REPORT_INTERVAL;
    double tdiff = (now - last_report)/1000.0; // in seconds
    LOG(INFO)<< "BWReporter@ " << (now/1000) << " " << bytes/tdiff << " bytes/sec; " <<
        tuples/tdiff << " tuples/sec";
#endif
    bytes = 0;
    tuples = 0;
    next_report = now + REPORT_INTERVAL;
  }
}


const std::string RemoteDestAdaptor::generic_name("Remote connection");

