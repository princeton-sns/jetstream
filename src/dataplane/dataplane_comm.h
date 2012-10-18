#ifndef JetStream_dataplane_comm_h
#define JetStream_dataplane_comm_h

#include <map>
#include <set>
#include <boost/shared_ptr.hpp>
#include <boost/thread.hpp>
#include <boost/date_time/posix_time/posix_time.hpp>


#include "connection.h"
#include "dataplaneoperator.h"
#include <glog/logging.h>

namespace  jetstream {
  

class DataplaneConnManager;



class RemoteDestAdaptor : public TupleReceiver {
  friend class DataplaneConnManager;

  const static int SIZE_TO_SEND = 4096;
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
                     msec_t wait_for_conn);
  virtual ~RemoteDestAdaptor() {
    timer.cancel();
    LOG(INFO) << "destructing RemoteDestAdaptor to " << dest_as_str;
 }

  virtual void process (boost::shared_ptr<Tuple> t);
  
  virtual void no_more_tuples();
  
  virtual boost::shared_ptr<CongestionMonitor> congestion_monitor();

  virtual const std::string& typename_as_str() {return generic_name;};
  virtual std::string long_description();
  virtual std::string id_as_str() {return long_description();}

  
  private:
   static const std::string generic_name;
  
  
  class QueueCongestionMonitor: public CongestionMonitor {
  
    private:
      static const size_t MAX_QUEUE_BYTES = 1E6;
      RemoteDestAdaptor& rda;

    public:
      QueueCongestionMonitor(RemoteDestAdaptor& o) : rda(o) {}
      virtual bool is_congested() {
      /*
        if (rda.chainIsReady) {
          std::cout << "queue size " << rda.conn->bytes_queued() << std::endl;
        } else
          std::cout << "waiting for conn in monitor \n";
          */
        return !(rda.chainIsReady) || ( rda.conn->bytes_queued() > MAX_QUEUE_BYTES);
      }
    
  };
    
};
  




/**
 * Instances of this class are responsible for managing incoming data on the
 * dataplane. 
 * They are also responsible for managing the lifetimes of the RemoteDestAdaptors
 * that handle OUTGOING traffic.
 */
class DataplaneConnManager {

/**
Internally, we identify endpoints by a string consisting of an address:port pair
*/
 private:
  std::map<std::string, std::vector<boost::shared_ptr<ClientConnection> > > pendingConns;
  
    /** Maps from remote endpoint to the local client-connection associated with it.
    * Note that the connection-to-destination mapping is implicit in the callback
    * closure and is not stored explicitly.
    */
  std::map<boost::asio::ip::tcp::endpoint, boost::shared_ptr<ClientConnection> > liveConns;

  boost::recursive_mutex incomingMapMutex;
  
  
  void got_data_cb (boost::shared_ptr<ClientConnection> c,
                    boost::shared_ptr<TupleReceiver> dest,
                    const DataplaneMessage &msg,
                    const boost::system::error_code &error);
  
  
  void operator= (const DataplaneConnManager &) 
    { LOG(FATAL) << "cannot copy a DataplaneConnManager"; }
  DataplaneConnManager (const DataplaneConnManager & d):iosrv(d.iosrv),strand(d.strand)
    { LOG(FATAL) << "cannot copy a DataplaneConnManager"; }
  
 public:
  DataplaneConnManager (boost::asio::io_service& io):iosrv(io), strand(iosrv) {}
 
 
    // called to attach incoming connection c to existing operator dest
  void enable_connection (boost::shared_ptr<ClientConnection> c,
                          boost::shared_ptr<TupleReceiver> dest);
                     

    // called to attach income connection c to an operator that doesn't yet exist
  void pending_connection (boost::shared_ptr<ClientConnection> c,
                          std::string future_op);

    // called when an operator is created
  void created_operator (boost::shared_ptr<TupleReceiver> dest);
                         
  void close();
  
  ///////////////  Handles outgoing connections ////////////
  
 public:
 /**
 * RemoteDestAdaptors need to be torn down once there is no more data for them.
 * For now, we just do this by blocking in RemoteDestAdaptor::no_more_tuples().
 * The below code is mostly dead but we are keeping in case we decide we need
 * a non-blocking solution.
 */
    void register_new_adaptor(boost::shared_ptr<RemoteDestAdaptor> p) {
       boost::lock_guard<boost::recursive_mutex> lock (outgoingMapMutex);
       adaptors[p->dest_as_str] = p;
    }
  
   void cleanup(std::string id) {
      strand.post (boost::bind(&DataplaneConnManager::deferred_cleanup,this, id));
    }
  
    void deferred_cleanup(std::string);
 
  private:
    boost::asio::io_service & iosrv;
    boost::asio::strand strand;
    boost::recursive_mutex outgoingMapMutex;
  /**
  * Maps from a destination operator ID to an RDA for it.
  */
    std::map<std::string, boost::shared_ptr<RemoteDestAdaptor> > adaptors;
  
  
};


}

#endif
