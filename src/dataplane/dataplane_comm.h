#ifndef JetStream_dataplane_comm_h
#define JetStream_dataplane_comm_h

#include <map>
#include <set>
#include <boost/shared_ptr.hpp>
#include <boost/thread.hpp>
#include <boost/date_time/posix_time/posix_time.hpp>
#include <glog/logging.h>


#include "connection.h"
#include "dataplaneoperator.h"
#include "node_config.h"
#include "queue_congestion_mon.h"

namespace  jetstream {
  

class DataplaneConnManager;



class RemoteDestAdaptor : public TupleReceiver {
  friend class DataplaneConnManager;

  const static size_t SIZE_TO_SEND = 4096;
  const static int WAIT_FOR_DATA = 50; //ms;
//  const static boost::posix_time::ptime WAIT_FOR_DATA =
//    boost::posix_time::milliseconds(50);
 private:
  DataplaneConnManager& mgr;
  boost::asio::io_service & iosrv;


  boost::shared_ptr<ClientConnection> conn;
  boost::condition_variable chainReadyCond;
  boost::mutex mutex;
  volatile bool chainIsReady;
  volatile size_t this_buf_size;
//  bool stopping;
  std::string dest_as_str;  //either operator ID or cube
  std::string remoteAddr;
  Edge dest_as_edge;
  DataplaneMessage msg;
  boost::asio::deadline_timer timer;
  boost::shared_ptr<TupleSender> pred;

  
  void conn_created_cb (boost::shared_ptr<ClientConnection> conn,
                        boost::system::error_code error);
  
  void conn_ready_cb (const DataplaneMessage &msg,
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
                     msec_t wait_for_conn, boost::shared_ptr<TupleSender>);
  virtual ~RemoteDestAdaptor() {
    timer.cancel();
    LOG(INFO) << "destructing RemoteDestAdaptor to " << dest_as_str;
 }

  virtual void process (boost::shared_ptr<Tuple> t);
  
  virtual void no_more_tuples();

  virtual void meta_from_upstream(const DataplaneMessage & msg, const operator_id_t pred);

  
  virtual const std::string& typename_as_str() {return generic_name;};
  virtual std::string long_description();
  virtual std::string id_as_str() {return long_description();}

  
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
  

class IncomingConnectionState {
  boost::shared_ptr<ClientConnection> conn;
  boost::shared_ptr<TupleReceiver> dest;
  boost::shared_ptr<CongestionMonitor> mon;
  boost::asio::io_service & iosrv;
  DataplaneConnManager& mgr;
  boost::asio::deadline_timer timer;
  operator_id_t remote_op;

public:
  void got_data_cb (const DataplaneMessage &msg,
                    const boost::system::error_code &error);
  
  IncomingConnectionState(boost::shared_ptr<ClientConnection> c,
                          boost::shared_ptr<TupleReceiver> d,
                          boost::asio::io_service & i,
                          DataplaneConnManager& m,
                          operator_id_t srcOpID):
      conn(c),dest(d), iosrv(i), mgr(m), timer(iosrv), remote_op(srcOpID) {
      mon = dest->congestion_monitor();
  }
  
  void close_async() {
    conn->close_async(no_op_v);
  }
  
  boost::asio::ip::tcp::endpoint get_remote_endpoint() {
    return conn->get_remote_endpoint();
  }
  
  void report_congestion_upstream(double congestionLevel);
  
  void register_congestion_recheck();

  void congestion_recheck_cb();


  virtual ~IncomingConnectionState() {
    timer.cancel();
  }
  
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
  
  
  void operator= (const DataplaneConnManager &) 
    { LOG(FATAL) << "cannot copy a DataplaneConnManager"; }
  DataplaneConnManager (const DataplaneConnManager & d):iosrv(d.iosrv),strand(d.strand),cfg(d.cfg)
    { LOG(FATAL) << "cannot copy a DataplaneConnManager"; }
  
 public:
  DataplaneConnManager (boost::asio::io_service& io, const NodeConfig& c):
      iosrv(io), strand(iosrv), cfg(c) {}
 
 
    // called to attach incoming connection c to existing operator dest
  void enable_connection (boost::shared_ptr<ClientConnection> c,
                          boost::shared_ptr<TupleReceiver> dest,
                          operator_id_t srcOpID);
                     

    // called to attach income connection c to an operator that doesn't yet exist
  void pending_connection (boost::shared_ptr<ClientConnection> c,
                          std::string future_op,
                          operator_id_t srcOpID);

    // called when an operator is created
  void created_operator (boost::shared_ptr<TupleReceiver> dest);
                         
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
      strand.post (boost::bind(&DataplaneConnManager::deferred_cleanup,this, id));
    }
  
    void deferred_cleanup(std::string);

   void cleanup_incoming(boost::asio::ip::tcp::endpoint c) {
      strand.post (boost::bind(&DataplaneConnManager::deferred_cleanup_in,this, c));
    }
  
    void deferred_cleanup_in(boost::asio::ip::tcp::endpoint);

    size_t maxQueueSize() { return cfg.sendQueueSize; }
 
  private:
    boost::asio::io_service & iosrv;
    boost::asio::strand strand;
    const NodeConfig& cfg;
    boost::recursive_mutex outgoingMapMutex;
  /**
  * Maps from a destination operator ID to an RDA for it.
  */
    std::map<std::string, boost::shared_ptr<RemoteDestAdaptor> > adaptors;
  
  
};


}

#endif
