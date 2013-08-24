#ifndef JetStream_dataplane_comm_h
#define JetStream_dataplane_comm_h

#include <map>
#include <set>
#include <boost/shared_ptr.hpp>
#include <boost/thread.hpp>
#include <boost/date_time/posix_time/posix_time.hpp>
#include <glog/logging.h>


#include "connection.h"
#include "node_config.h"
#include "queue_congestion_mon.h"
#include "window_congest_mon.h"
#include "counter.h"
#include "operator_chain.h"

#undef ACK_EACH_PACKET
#undef ACK_WINDOW_END

namespace  jetstream {
  

class DataplaneConnManager;

class BWReporter {
  unsigned tuples;
  unsigned bytes;
  msec_t next_report;
  msec_t REPORT_INTERVAL;
  public:
    BWReporter(): tuples(0), bytes(0),next_report(0),REPORT_INTERVAL(5000)
    {}
    void sending_a_tuple(size_t b);
    
};


class RemoteDestAdaptor : public ChainMember {
  friend class DataplaneConnManager;

  const static size_t SIZE_TO_SEND = 4096;
  const static int WAIT_FOR_DATA = 50; //ms;
//  const static boost::posix_time::ptime WAIT_FOR_DATA =
//    boost::posix_time::milliseconds(50);
 private:
  DataplaneConnManager& mgr;
  //boost::asio::io_service & iosrv;

  boost::shared_ptr<NetCongestionMonitor> local_congestion;
  boost::shared_ptr<ClientConnection> conn;
  boost::condition_variable chainReadyCond;
  boost::mutex mutex;
  volatile bool chainIsReady;
  volatile size_t this_buf_size;
  volatile bool is_stopping;
//  bool stopping;
  std::string dest_as_str;  //either operator ID or cube
  std::string remoteAddr;
  Edge dest_as_edge;
  DataplaneMessage out_buffer_msg;
  boost::asio::deadline_timer timer;
  BWReporter reporter;
  std::vector<boost::shared_ptr<OperatorChain> > chains;
  
  
  void conn_created_cb (boost::shared_ptr<ClientConnection> conn,
                        boost::system::error_code error);
  
  void conn_ready_cb (DataplaneMessage &msg,
                      const boost::system::error_code &error);

  bool wait_for_chain_ready ();
   
  msec_t wait_for_conn; // Note this is a wide area wait.
  void force_send(); //called by timer
  void do_send_unlocked(); //does the send, no lock acquisition

  
 public:
  //  The below ctor might be useful at some future point, but for now we aren't
  //    using it and therefore won't have it enabled
//  RemoteDestAdaptor (boost::shared_ptr<ClientConnection> c) :
//      conn (c), chainIsReady(false), stopping(false) {}

  RemoteDestAdaptor (DataplaneConnManager &n, ConnectionManager &cm,
                       boost::asio::io_service & iosrv, const jetstream::Edge&,
                     msec_t wait_for_conn);
  virtual ~RemoteDestAdaptor(); 

//  virtual void process (boost::shared_ptr<Tuple> t, const operator_id_t src);
  virtual void process_delta (Tuple& oldV, boost::shared_ptr<Tuple> newV, const operator_id_t pred);
  virtual void process(OperatorChain * chain, std::vector<boost::shared_ptr<Tuple> > &, DataplaneMessage&);
  virtual bool is_source() {return false;}
  
  
  virtual void chain_stopping(OperatorChain * );

  virtual void meta_from_upstream(const DataplaneMessage & msg);

  virtual void add_chain(boost::shared_ptr<OperatorChain> c) {
    chains.push_back(c);
  }
  
  void connection_broken(); //called on a broken connection; does teardown.
  
  virtual const std::string& typename_as_str() const {return generic_name;};
  virtual std::string long_description() const;
  virtual std::string id_as_str() const {return long_description();}

  
  private:
   static const std::string generic_name;  
  
 public:
  virtual boost::shared_ptr<CongestionMonitor> congestion_monitor() {
    if (!wait_for_chain_ready()) {
      boost::shared_ptr<CongestionMonitor> m;
      return m; //no monitor
    }
    return conn->congestion_monitor();
  }
  
};
  

class IncomingConnectionState: public ChainMember {
  boost::shared_ptr<ClientConnection> conn;
  boost::asio::io_service & iosrv;
  DataplaneConnManager& mgr;
  boost::asio::deadline_timer timer;
  operator_id_t remote_op;
  boost::shared_ptr<OperatorChain> dest;
  boost::shared_ptr<WindowCongestionMonitor> dest_side_congest;
  boost::shared_ptr<CongestionMonitor> chain_mon;

public:
  void got_data_cb (DataplaneMessage &msg,
                    const boost::system::error_code &error);
  
  IncomingConnectionState(boost::shared_ptr<ClientConnection> c,
                          boost::shared_ptr<OperatorChain> d,
                          boost::asio::io_service & i,
                          DataplaneConnManager& m,
                          operator_id_t srcOpID);
  
  
  virtual bool is_source() {return true;}
  virtual void process(OperatorChain * chain, std::vector<boost::shared_ptr<Tuple> > &, DataplaneMessage&) {}
  
  void close_async() {
    timer.cancel();
    conn->close_async(no_op_v);
  }
  
  
  void close_now() {
    timer.cancel();
    conn->close_now();
  }
  
  boost::asio::ip::tcp::endpoint get_remote_endpoint() {
    return conn->get_remote_endpoint();
  }
  
  boost::asio::strand * get_strand() {
    return conn->get_recv_strand();
  }
  
  void report_congestion_upstream(double congestionLevel);
  
  void register_congestion_recheck();

  void congestion_recheck_cb(const boost::system::error_code& error);


  virtual ~IncomingConnectionState() {
    timer.cancel();
  }
  
  virtual void meta_from_downstream(DataplaneMessage & msg);
  
  virtual void chain_is_broken() {
    LOG(INFO) << "closing down incoming socket due to chain-broken ahead";
    close_async();
  }
  
  virtual std::string id_as_str() const {
    std::ostringstream o;
    o << "Connection from " << remote_op;
    if (conn)
      o << " on " << conn->get_remote_endpoint();
    return o.str();
  }
  
  virtual const std::string& typename_as_str() const {
    return generic_name; 
  }; //return a name for the type
  
  
  private:
   void no_more_tuples();//  called on error to do teardown
   const static std::string generic_name;
  
};


struct PendingConn {
  boost::shared_ptr<ClientConnection>  conn;
  operator_id_t src;
  
  PendingConn(boost::shared_ptr<ClientConnection>  c, operator_id_t s):
    conn(c), src(s) {}
};


/**
 * Instances of this class are responsible for managing incoming data on the
 * dataplane. 
 * They are also responsible for managing the lifetimes of the RemoteDestAdaptors
 * that handle OUTGOING traffic.
 */
class DataplaneConnManager {

/**
Internally, we identify endpoints by a string consisting of either an operator ID or a cube name
*/
 private:
  std::map<std::string, std::vector<PendingConn> > pendingConns;
  
    /** Maps from remote endpoint to the local client-connection associated with it.
    * Note that the connection-to-destination mapping is implicit in the callback
    * closure and is not stored explicitly.
    */
  std::map<boost::asio::ip::tcp::endpoint, boost::shared_ptr<IncomingConnectionState> > liveConns;

  boost::recursive_mutex incomingMapMutex;
  Node * node;
  
  void operator= (const DataplaneConnManager &) 
    { LOG(FATAL) << "cannot copy a DataplaneConnManager"; }
  DataplaneConnManager (const DataplaneConnManager & d):iosrv(d.iosrv),strand(d.strand),cfg(d.cfg)
    { LOG(FATAL) << "cannot copy a DataplaneConnManager"; }
  
 public:
  DataplaneConnManager (boost::asio::io_service& io, Node * n);
 
 
    // called to attach incoming connection c to existing operator dest
  void enable_connection (boost::shared_ptr<ClientConnection> c,
                          boost::shared_ptr<OperatorChain> dest,
                          operator_id_t srcOpID);
                     

    // called to attach income connection c to an operator that doesn't yet exist
  void pending_connection (boost::shared_ptr<ClientConnection> c,
                          std::string future_op,
                          operator_id_t srcOpID);

    // called when a non-source operator chain is created.
    // The argument is a sourceless chain
  void created_chain (boost::shared_ptr<OperatorChain> dest);
                         
  void close();
    
  ///////////////  Handles outgoing connections ////////////
  
 public:
 /**
 * RemoteDestAdaptors need to be torn down once there is no more data for them.
 * Similarly, we need to tear down incoming connections
 */
    void register_new_adaptor(boost::shared_ptr<RemoteDestAdaptor> p) {
       boost::lock_guard<boost::recursive_mutex> lock (outgoingMapMutex);
       adaptors[p->dest_as_str] = p;
    }
  
    void cleanup(std::string id) {
//      LOG(INFO) << "deferring cleanup for " << id;
      strand.post (boost::bind(&DataplaneConnManager::deferred_cleanup,this, id));
    }
 
    void deferred_cleanup(std::string);

    void cleanup_incoming(boost::asio::ip::tcp::endpoint c) {
      strand.post (boost::bind(&DataplaneConnManager::deferred_cleanup_in,this, c));
    }
  
    void deferred_cleanup_in(boost::asio::ip::tcp::endpoint);

    size_t maxQueueSize() { return cfg.send_queue_size; }

    Counter * send_counter, *recv_counter;
    void set_counters(Counter * s, Counter * r) {
      send_counter = s;
      recv_counter = r;
    }  
 
    Node * get_node() {   return node;  }
    boost::recursive_mutex outgoingMapMutex;

  private:
    boost::asio::io_service & iosrv;
    boost::asio::strand strand;
    const NodeConfig& cfg;
  /**
  * Maps from a destination operator ID to an RDA for it.
    We need this so that an RDA can arrange to tear down the chain it's a member of.
    Want the chain to stop, followed by conn teardown.
  */
    std::map<std::string, boost::shared_ptr<RemoteDestAdaptor> > adaptors;
  
};

}

#endif
