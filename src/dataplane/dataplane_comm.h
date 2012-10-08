#ifndef JetStream_dataplane_comm_h
#define JetStream_dataplane_comm_h

#include <map>
#include <set>
#include <boost/shared_ptr.hpp>
#include <boost/thread.hpp>

#include "connection.h"
#include "dataplaneoperator.h"
#include <glog/logging.h>

namespace  jetstream {
  

class DataplaneConnManager;

class RemoteDestAdaptor : public TupleReceiver {
  friend class DataplaneConnManager;


 private:
  DataplaneConnManager& mgr;
 
  boost::shared_ptr<ClientConnection> conn;
  boost::condition_variable chainReadyCond;
  boost::mutex mutex;
  bool chainIsReady;
//  bool stopping;
  std::string dest_as_str;  //either operator ID or cube
  std::string remoteAddr;
  Edge dest_as_edge;
  
  void conn_created_cb (boost::shared_ptr<ClientConnection> conn,
                        boost::system::error_code error);
  
  void conn_ready_cb (const DataplaneMessage &msg,
                      const boost::system::error_code &error);
   
  msec_t wait_for_conn; // Note this is a wide area wait.
  
 public:
  //  The below ctor might be useful at some future point, but for now we aren't
  //    using it and therefore won't have it enabled
//  RemoteDestAdaptor (boost::shared_ptr<ClientConnection> c) :
//      conn (c), chainIsReady(false), stopping(false) {}

  RemoteDestAdaptor (DataplaneConnManager &n, ConnectionManager &cm,
                     const jetstream::Edge&,
                     msec_t wait_for_conn);
  virtual ~RemoteDestAdaptor() {
    LOG(INFO) << "destructing RemoteDestAdaptor";
 }

  virtual void process (boost::shared_ptr<Tuple> t);
  
  virtual void no_more_tuples();

  
  virtual const std::string& typename_as_str() {return generic_name;};
  virtual std::string long_description();
  virtual std::string id_as_str() {return long_description();}

  
  private:
   static const std::string generic_name;
};
  

/**
 * Instances of this class are responsible for managing incoming data on the
 * dataplane. 
 * They are also responsible for managing the lifetimes of the RemoteDestAdaptors
 * that handle OUTGOING traffic.
 */
class DataplaneConnManager {

 private:
  std::map<std::string, std::vector<boost::shared_ptr<ClientConnection> > > pendingConns;
  
    /** Maps from remote endpoint to the local client-connection associated with it.
    * Note that the connection-to-destination mapping is implicit in the callback
    * closure and is not stored explicitly.
    */
  std::map<boost::asio::ip::tcp::endpoint, boost::shared_ptr<ClientConnection> > liveConns;
  
  
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
    void register_new_adaptor(boost::shared_ptr<RemoteDestAdaptor> p) {
       adaptors[p->dest_as_str] = p;
    }
  
/*    //currently dead code
   void cleanup(string id) {
      strand.post (boost::bind(&DataplaneConnManager::deferred_cleanup,this, id));
    }
  */

/*  Currently dead code.  */
    void deferred_cleanup(std::string);
 
  private:
    boost::asio::io_service & iosrv;
    boost::asio::strand strand;
  /**
  * Maps from a destination operator ID to an RDA for it.
  * This 
  */
    std::map<std::string, boost::shared_ptr<RemoteDestAdaptor> > adaptors;
  
  
};


}

#endif
