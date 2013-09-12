#include <glog/logging.h>
#include "dataplane_comm.h"
#include "node.h"
#include "sharedwindow_congest_mon.h"

using namespace jetstream;
//using namespace std;
using std::string;
using std::vector;
using std::map;
using namespace boost;
using namespace boost::asio::ip;

#undef REPORT_BW
#undef ACK_WINDOW_END
#define STATUS_ON_WINDOW_END 1

IncomingConnectionState::IncomingConnectionState(boost::shared_ptr<ClientConnection> c,
                          boost::shared_ptr<OperatorChain> d,
                          boost::asio::io_service & i,
                          DataplaneConnManager& m,
                          operator_id_t srcOpID):
      conn(c), iosrv(i), mgr(m), timer(iosrv), remote_op(srcOpID) {
  dest = d;
  string name = "local processing for remote from " + srcOpID.to_string();
  dest_side_congest = boost::shared_ptr<WindowCongestionMonitor>
//    (new WindowMonFacade(name, 0.5));
    (new WindowCongestionMonitor(name, 0.75));

  chain_mon = d->congestion_monitor();
}

void
IncomingConnectionState::no_more_tuples() {
  if (!dest)
    return;
  dest->stop_from_within();
  dest.reset();
  //can tear down socket now that chain is stopped and we no longer use its strand
  
//  DataplaneMessage teardown;
//  teardown.set_type(DataplaneMessage::NO_MORE_DATA);
//  dest->meta_from_upstream(teardown);
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
  long data_size_bytes = 0;
  for(int i=0; i < msg.data_size(); ++i) {
    shared_ptr<Tuple> d (new Tuple);
    d->MergeFrom (msg.data(i));
    assert (d->e_size() > 0);
    data.push_back(d);
    data_size_bytes += d->ByteSize();
  }

  switch (msg.type ()) {
  case DataplaneMessage::DATA:
    {
//      if (dest_side_congest->is_congested()) {
//        VLOG(1) << "reporting downstream congestion at " << dest->chain_name();
//        report_congestion_upstream(dest_side_congest->capacity_ratio());
//        register_congestion_recheck();
 //     }
//      LOG(INFO) << "GOT DATA; length is " << msg.data_size() << "tuples";
      dest_side_congest->report_insert(NULL, data_size_bytes); //this will start the timer if not yet started
      dest->process(data, msg);
      
      /* FIXME CHAINS
      for (int i = 0; i < msg.old_val_size(); ++i) {
        shared_ptr<Tuple> new_data (new Tuple);
        new_data->MergeFrom (msg.new_val(i));
        Tuple old_data;
        old_data.MergeFrom(msg.old_val(i));
        dest->process_delta(old_data, new_data, remote_op);      
      }*/
      break;
    }
  case DataplaneMessage::NO_MORE_DATA:
    {
      LOG(INFO) << "got no-more-data signal from " << conn->get_remote_endpoint()
                << ", will tear down connection into " << dest->chain_name();
      no_more_tuples();
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
      dest_side_congest->end_of_window(msg.window_length_ms());

      DataplaneMessage resp;
      boost::system::error_code err;      
      resp.set_timestamp(msg.timestamp());
      
#ifdef ACK_WINDOW_END
      LOG_EVERY_N(INFO, 40) << " got an end-of-window marker, acking it; ts was " << msg.timestamp()
       << " and window size was " << msg.window_length_ms();
      resp.set_type(DataplaneMessage::ACK);
      resp.set_window_length_ms(msg.window_length_ms());
      
      conn->send_msg(resp, err); // just echo back what we got
#endif
#ifdef STATUS_ON_WINDOW_END
      resp.set_type(DataplaneMessage::CONGEST_STATUS);
      resp.set_congestion_level(dest_side_congest->capacity_ratio());
      conn->send_msg(resp, err);
      break;
    }
#endif
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

  LOG(INFO) << "Reporting congestion at " << dest->chain_name()<< ": " << level;
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
  timer.expires_from_now(boost::posix_time::millisec(500));
  timer.async_wait(boost::bind(&IncomingConnectionState::congestion_recheck_cb, this, _1));
}


void
IncomingConnectionState::congestion_recheck_cb(const boost::system::error_code& error) {
  if (error == boost::asio::error::operation_aborted)
    return;

//  VLOG(1) << "rechecking congestion at "<< dest->id_as_str();
  double downstream_level = chain_mon->capacity_ratio();
  msec_t measurement_age = chain_mon->measurement_time();
  dest_side_congest->set_downstream_congestion(downstream_level, measurement_age);
  if (conn->is_connected()) {
    report_congestion_upstream(dest_side_congest->capacity_ratio());
//  if (!mon->is_congested()) // only report if congestion went away
//    report_congestion_upstream(0);
//  else
    register_congestion_recheck();
  }
}



void
IncomingConnectionState::meta_from_downstream(DataplaneMessage & msg) {
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
}

DataplaneConnManager::DataplaneConnManager (boost::asio::io_service& io, Node * n):
      node(n),send_counter(NULL), recv_counter(NULL),iosrv(io), strand(iosrv), cfg(node->cfg())
      {}


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
    dest->set_start(incomingConn);
    dest->strand = incomingConn->get_strand();
    dest->start();
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
  LOG(INFO) << "chain-ready sent";
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
DataplaneConnManager::created_chain (shared_ptr<OperatorChain> dest) {

  {
    lock_guard<boost::recursive_mutex> lock (incomingMapMutex);
    string op_id = dest->member(0)->id_as_str();
    map<string, vector<PendingConn> >::iterator pending_conn = pendingConns.find(op_id);
    if (pending_conn != pendingConns.end()) {
      LOG(INFO) << "Found match for pending chain " << op_id;

      vector< PendingConn > & conns = pending_conn->second;
      for (u_int conn = 0; conn < conns.size(); ++conn) {
        shared_ptr<OperatorChain> newchain = shared_ptr<OperatorChain>(new OperatorChain);
        newchain->add_member();
        newchain->clone_from(dest);
        for (unsigned op_id = 1; op_id < newchain->members(); ++op_id) {
          shared_ptr<ChainMember> m = newchain->member(op_id);
          LOG_IF(INFO, !m) << "member " << op_id << " of chain is undefined";
          m->add_chain(newchain);
        }
        enable_connection(conns[conn].conn, newchain, conns[conn].src);
      }
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
  : mgr(dcm), chainIsReady(false), this_buf_size(0), is_stopping(false), 
    timer(io),no_window_started(true), wait_for_conn(wait) {
  int32_t portno = e.dest_addr().portno();

  remoteAddr = e.dest_addr().address()+ ":" + lexical_cast<string>(portno);
  
  dest_as_edge.CopyFrom(e);
  if (dest_as_edge.has_dest()) {
    operator_id_t destOpId;
    destOpId.computation_id = e.computation();
    destOpId.task_id = e.dest();
    dest_as_str = destOpId.to_string();
  }
  else if (dest_as_edge.has_dest_cube()){
    dest_as_str = dest_as_edge.dest_cube();
  } else
    dest_as_str = "external"; // + remoteAddr ;
  
//  local_congestion = boost::shared_ptr<NetCongestionMonitor> (
//        new QueueCongestionMonitor(dcm.maxQueueSize(), dest_as_str));
  
  cm.create_connection(e.dest_addr().address(), portno, boost::bind(
                 &RemoteDestAdaptor::conn_created_cb, this, _1, _2));
      
}

void
RemoteDestAdaptor::conn_created_cb(shared_ptr<ClientConnection> c,
                                     boost::system::error_code error) {
  if (error || ! c->is_connected()) {
    LOG(WARNING) << "Dataplane connection failed: " << error.message();
    return;
  }
  
  LOG(INFO) << "RDA to " << c->get_remote_endpoint() <<  " set up OK";
  conn = c;
  
  conn->set_counters(mgr.send_counter, mgr.recv_counter);

  DataplaneMessage data_msg;
  data_msg.set_type(DataplaneMessage::CHAIN_CONNECT);
  
  std::ostringstream mon_name;
  mon_name << "connection to " << c->get_remote_endpoint();

/*
#ifdef ACK_EACH_PACKET
  remote_processing = boost::shared_ptr<QueueCongestionMonitor>(
    new QueueCongestionMonitor(mgr.maxQueueSize(), mon_name.str()));
#endif
#ifdef ACK_WINDOW_END
  remote_processing = boost::shared_ptr<WindowCongestionMonitor>(
    new WindowCongestionMonitor(mon_name.str()));
#endif
  if(dest_as_edge.has_max_kb_per_sec())
     remote_processing->set_max_rate(dest_as_edge.max_kb_per_sec() * 1000); //convert kb --> bytes

//*/
  local_congestion = conn->congestion_monitor();
  conn->congestion_monitor()->set_queue_size(mgr.maxQueueSize());
  if(dest_as_edge.has_max_kb_per_sec())
    conn->congestion_monitor()->set_max_rate(dest_as_edge.max_kb_per_sec() * 1000);

  Edge * edge = data_msg.mutable_chain_link();
  edge->CopyFrom(dest_as_edge);
  
  
  boost::system::error_code err;
  conn->recv_data_msg(boost::bind(&RemoteDestAdaptor::conn_ready_cb, 
				  this, _1, _2), err);
  conn->send_msg(data_msg, err);
  LOG(INFO) << "Chain connect sent";
}

void
RemoteDestAdaptor::conn_ready_cb(DataplaneMessage &msg,
                                        const boost::system::error_code &error) {

  if (error) {
    if (error != boost::system::errc::operation_canceled) 
      LOG(WARNING) << error.message() << " code = " << error.value();
    connection_broken();
    return;
  }

  LOG_IF(FATAL, !chainIsReady && msg.type() != DataplaneMessage::CHAIN_READY) <<
    "Chain-is-ready must be first message on connection after creation";
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
      msec_t age_ms = get_msec() - msg.timestamp();
      LOG(INFO) << "Received remote congestion report from " <<  dest_as_str
        <<" : status is " << status << " at " << msg.timestamp()<< " (age = "
        << age_ms << " ms)";

      local_congestion->set_downstream_congestion(status, msg.timestamp());
      break;
    }

    case DataplaneMessage::TPUT_START:
    case DataplaneMessage::TPUT_ROUND_2:
    case DataplaneMessage::TPUT_ROUND_3:
    case DataplaneMessage::SET_BACKOFF:
    {
      for (unsigned i = 0; i < chains.size(); ++i)
        chains[i]->upwards_metadata(msg, this);
//      pred->meta_from_downstream(msg);
      break;
    }

    case DataplaneMessage::ACK:
    {    
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
RemoteDestAdaptor::process_delta (Tuple& oldV, boost::shared_ptr<Tuple> newV, const operator_id_t pred) {
  if (!wait_for_chain_ready()) {
    LOG(WARNING) << "timeout on dataplane connection to " << dest_as_str
		 << ". Aborting data message send. Should queue/retry instead?";
    return;
  }

  {
    size_t sz = oldV.ByteSize() + newV->ByteSize();
    reporter.sending_a_tuple(sz);

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
  
  if (is_stopping)
    return;
  if (!wait_for_chain_ready()) {
    LOG(WARNING) << "timeout on dataplane connection to " << dest_as_str
		 << ". Aborting data message send. Should queue/retry instead?";
    return;
  }

  {
    unique_lock<boost::mutex> lock(mutex); //lock around buffers
    
    if (no_window_started) {
      no_window_started = false;
      DataplaneMessage window_start;
      window_start.set_type(DataplaneMessage::DATA);
      boost::system::error_code err;
      conn->send_msg(window_start, err);
        //send an empty message as a header for the window
    }
    
    out_buffer_msg.set_type(DataplaneMessage::DATA);

    bool buffer_was_empty = (this_buf_size == 0);
    
    for(unsigned i = 0; i < tuples.size(); ++i) {
      boost::shared_ptr<Tuple> t = tuples[i];
      if (!t)
        continue;
      size_t sz = t->ByteSize();
      reporter.sending_a_tuple(sz);
//      remote_processing->report_insert(0, sz);
      this_buf_size += sz;
      out_buffer_msg.add_data()->MergeFrom(*t);
    }
    
    if (msg.has_type()) {
      do_send_unlocked();   //saw a meta marker, so force out the data
      meta_from_upstream(msg); 
    } else if (this_buf_size > SIZE_TO_SEND) {
      do_send_unlocked();
    } else {
      if (buffer_was_empty) {  //no meta, and buffer isn't full
        timer.expires_from_now(boost::posix_time::millisec(WAIT_FOR_DATA));
        timer.async_wait(boost::bind(&RemoteDestAdaptor::force_send, this));
      }
    }
  }

}



void
RemoteDestAdaptor::force_send() {
  unique_lock<boost::mutex> lock(mutex);
  if (this_buf_size == 0 || is_stopping)
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
    LOG(WARNING) << "Send failed; tearing down chain to " << dest_as_str; //; local end is " << pred->id_as_str();
    connection_broken();
    conn->close_async(boost::bind(&DataplaneConnManager::cleanup, &mgr, dest_as_str));
  }
  
}

void
RemoteDestAdaptor::connection_broken () {
  if (is_stopping) //already stopping
    return;
  
  LOG(INFO) << "connection unexpectedly broken to " << dest_as_str << ", will tear down.";
  is_stopping = true;
  timer.cancel();
  conn.reset();
  boost::unique_lock<recursive_mutex>(outgoingMapMutex);
    //note that we hold the lock until we're done here, so we can't get destroyed prematurely
  mgr.cleanup(dest_as_str);
  
  for(unsigned i = 0; i < chains.size(); ++i) {
    if (chains[i]) {
      Node * n = mgr.get_node();
      shared_ptr<OperatorChain> c = chains[i];
      chains[i]->stop();  //we are NOT on the source-strand for the chain
        //chains[i] is implicitly cleared here by chain_stopping
      n->unregister_chain(c);
    }
  }
  chains.clear();
  
}

void
data_noop_cb(DataplaneMessage &msg, const boost::system::error_code &error) {
}

void
RemoteDestAdaptor::chain_stopping (OperatorChain * c) {
    
  if (!conn || !conn->is_connected() )
    return;

  int active_chains = 0;
  for (unsigned i = 0; i < chains.size(); ++i) {
    if (chains[i].get() == c) {
      chains[i].reset(); //remove the chain from our cache.
    }
    if (chains[i])
      active_chains ++;
  } 
  
  if (active_chains == 0) {
    if (!is_stopping) {
      force_send();
      timer.cancel();//we just pushed out all data
      DataplaneMessage d;
      d.set_type(DataplaneMessage::NO_MORE_DATA);
      
      boost::system::error_code err;
      conn->send_msg(d, err); //on strand, so safe?
      LOG(INFO) << "sent last data from connection (total is " << conn->send_count() << " bytes for node as a whole); queueing for teardown.";
    }

    boost::system::error_code error;  
    conn->recv_data_msg(boost::bind(data_noop_cb, _1, _2), error);
    conn->close_async(boost::bind(&DataplaneConnManager::cleanup, &mgr, dest_as_str));
  }
}

void
RemoteDestAdaptor::meta_from_upstream(const DataplaneMessage & msg_in) {

  boost::system::error_code err;
  LOG_IF(FATAL, msg_in.type() == DataplaneMessage::NO_MORE_DATA)  << "Shouldn't get no-more-data as routine meta";
  
#ifdef STATUS_ON_WINDOW_END
  if (msg_in.type() == DataplaneMessage::END_OF_WINDOW &&
      msg_in.has_window_length_ms()) {
    
    if (no_window_started) { //no data in window
    
      LOG(INFO) << "end of window with no data";
      conn->send_msg(msg_in, err);
    } else {
      no_window_started = true; //window over, so we reset the flag
    
      DataplaneMessage msg_out;
      msg_out.CopyFrom(msg_in);
      msg_out.set_timestamp(  get_msec()  );
//      LOG(INFO) << "Sending out end-of-window. window start ts = " << msg_out.timestamp();
      //we are on caller's thread so this is thread-safe.
      conn->send_msg(msg_out, err);
    }
  }
  else {
#endif
    //we are on caller's thread so this is thread-safe.
    conn->send_msg(msg_in, err);
#ifdef STATUS_ON_WINDOW_END
  }
#endif
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
    if (!conn_established || is_stopping) {
      return false;
    } 
  }
  return true;
}

string
RemoteDestAdaptor::long_description() const {
    std::ostringstream buf;
    buf << dest_as_str << " on " << remoteAddr <<
       (chainIsReady ? " (ready)" : " (waiting for dest)");
    return buf.str();
}

RemoteDestAdaptor::~RemoteDestAdaptor() {
  timer.cancel();
  LOG(INFO) << "destructor for RemoteDestAdaptor to " << dest_as_str;
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
  LOG(INFO) << "erasing / destructing adaptor to " << a->dest_as_str;
  if (!a->conn || !a->conn->is_connected()) {
    adaptors.erase(id);
  } else {
    LOG(FATAL) << "need to handle deferred cleanup of an in-use rda";
   //should set this up on a timer
  }
  //destructor fires HERE, when shared pointer goes out of scope
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
const std::string IncomingConnectionState::generic_name("Incoming");

